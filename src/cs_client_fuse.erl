-module(cs_client_fuse).

-behaviour(gen_event).

%% public api

-export([install/0]).
-export([ask/0]).
-export([melt/1]).

%% gen_event api

-export([init/1,
         handle_event/2,
         handle_call/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

%% types

-define(INET, cs_client_inet).
-define(PACKET, cs_client_packet).

%% public api

install() ->
    case ensure(?INET, client_inet_fuse) of
        ok                 -> ensure(?PACKET, client_packet_fuse);
        {error, _} = Error -> Error
    end.

ask() ->
    case fuse:ask(?INET, sync) of
        ok    -> fuse:ask(?PACKET, sync);
        blown -> blown
    end.

melt({inet, _}) ->
    fuse:melt(?INET);
melt({packet, _}) ->
    fuse:melt(?PACKET);
melt({drop, _}) ->
    ok.

%% gen_event api

init(_) ->
    update(fuse:ask(?INET, sync), fuse:ask(?PACKET, sync)).

handle_event({?INET, Inet}, {_, Packet}) ->
    update(Inet, Packet);
handle_event({?PACKET, Packet}, {Inet, _}) ->
    update(Inet, Packet);
handle_event(_, State) ->
    {ok, State}.

handle_call(Req, State) ->
    error(badcall, [Req, State]).

handle_info(_, State) ->
    {ok, State}.

code_change(_, State, _) ->
    {ok, State}.

terminate(_, State) ->
    State.

%% internal

ensure(Fuse, Key) ->
    case fuse:install(Fuse, fuse(Key)) of
        reset -> ok;
        Other -> Other
    end.

fuse(Key) ->
    application:get_env(cliserv, Key, {{standard, 5, 10}, {reset, 30000}}).

update(ok, ok) ->
    update(ok),
    {ok, {ok, ok}};
update(Inet, Packet) ->
    update(blown),
    {ok, {Inet, Packet}}.

update(Status) ->
    _ = [Pid ! {?MODULE, Status} ||
         {_, Pid, _, _} <- cs_client_pool:which_children(),
         is_pid(Pid)],
    ok.
