-module(cs_client).

%% public api

-export([call/1,
         call/2,
         cast/1,
         format_error/1]).

%% private api

-export([dynamic_ask_r/2,
         async_ask_r/2,
         cancel/2]).

%% types

-define(TIMEOUT, 5000).

-type reason() :: {drop, non_neg_integer()} |
                  {inet, closed | timeout | inet:posix()} |
                  {packet, {baddata, iodata()} | cs_packet:packet()}.

-export_type([reason/0]).

-callback call(Ref, Info, BinReq, Timeout) ->
    {ok, BinResp} | {exception, BinError}  |
    {error, Reason} | {exit, ExitReason} when
      Ref :: reference(),
      Info :: term(),
      BinReq :: binary(),
      Timeout :: timeout(),
      BinResp :: binary(),
      BinError :: binary(),
      Reason :: reason(),
      ExitReason :: term().

-callback cast(Ref, Info, BinReq) -> ok | {error, Reason} when
      Ref :: reference(),
      Info :: term(),
      BinReq :: binary(),
      Reason :: reason().

%% public api

-spec call(Req) ->
    {ok, Result} | Exception | {error, Reason} when
      Req :: term(),
      Result :: term(),
      Exception :: {error | throw | exit, term(), list()},
      Reason :: reason().
call(Req) ->
    call(Req, ?TIMEOUT).

-spec call(Req, Timeout) ->
    {ok, Result} | Exception | {error, Reason} when
      Req :: term(),
      Timeout :: timeout(),
      Result :: term(),
      Exception :: {error | throw | exit, term(), list()},
      Reason :: reason().
call(Req, Timeout) ->
    BinReq = term_to_binary(Req),
    case call_ask(BinReq, Timeout) of
        {ok, BinResp}         -> {ok, binary_to_term(BinResp)};
        {exception, BinError} -> decode_exception(BinError);
        {error, _} = Error    -> Error;
        {exit, Reason}        -> exit({Reason, {?MODULE, call, [Req, Timeout]}})
    end.

-spec cast(Req) ->
    ok | {error, Reason} when
      Req :: term(),
      Reason :: reason().
cast(Req) ->
    BinReq = term_to_binary(Req),
    cast_ask(BinReq).

-spec format_error(Reason) -> iodata() when
      Reason :: reason().
format_error({drop, Native}) ->
    MS = erlang:convert_time_unit(Native, native, millisecond),
    io_lib:format("dropped after ~bms", [MS]);
format_error({inet, closed}) ->
    "closed";
format_error({inet, timeout}) ->
    "timeout";
format_error({inet, Reason}) ->
    inet:format_error(Reason);
format_error({packet, {baddata, Data}}) ->
    io_lib:format("invalid data: ~w", [Data]);
format_error({packet, Packet}) ->
    io_lib:format("unexpected packet: ~p", [Packet]).

%% private api

dynamic_ask_r(Mod, Info) ->
    sbroker:dynamic_ask_r(?MODULE, {Mod, Info}).

async_ask_r(Mod, Info) ->
    sbroker:async_ask_r(?MODULE, {Mod, Info}).

cancel(Pid, MRef) ->
    sbroker:cancel(Pid, MRef).

%% internal

call_ask(BinReq, Timeout) ->
    case sbroker:ask(?MODULE, {call, self()}) of
        {go, Ref, {Mod, Info}, _, _} ->
            Mod:call(Ref, Info, BinReq, Timeout);
        {drop, _} = Drop ->
            {error, Drop}
    end.

cast_ask(BinReq) ->
    case sbroker:ask(?MODULE, {cast, self()}) of
        {go, Ref, {Mod, Info}, _, _} ->
            Mod:cast(Ref, Info, BinReq);
        {drop, _} = Drop ->
            {error, Drop}
    end.

decode_exception(BinError) ->
    case binary_to_term(BinError) of
        {error, _, _} = Error -> Error;
        {exit, _, _} = Exit   -> Exit;
        {throw, _, _} = Throw -> Throw;
        _Other                -> error(badarg, [BinError])
    end.
