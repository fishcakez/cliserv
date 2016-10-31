-module(cs_server).

-behaviour(acceptor).
-behaviour(gen_statem).

%% acceptor api

-export([acceptor_init/3,
         acceptor_continue/3,
         acceptor_terminate/2]).

%% gen_statem api

-export([callback_mode/0,
         handle_event/4,
         code_change/4,
         terminate/3]).

%% types

-callback init(Arg) -> {ok, State} when
      Arg :: term(),
      State :: term().

-callback handle_call(Req, From, State) ->
    {reply, Resp, NState} | {exception, Error, NState} when
      Req :: term(),
      From :: {pid(), term()},
      State :: term(),
      Resp :: term(),
      Error :: {error | exit | throw, term(), list()},
      NState :: term().

-callback handle_cast(Req, State) -> {noreply, NState} when
      Req :: term(),
      State :: term(),
      NState :: term().

-callback handle_info(Msg, State) -> {noreply, NState} when
      Msg :: term(),
      State :: term(),
      NState :: term().

-callback code_change(OldVsn, State, Extra) -> {ok, NState} when
      OldVsn :: term(),
      State :: term(),
      Extra :: term(),
      NState :: term().

-callback terminate(Reason, State) -> Ignored when
      Reason :: term(),
      State :: term(),
      Ignored :: term().

-record(data, {sock :: gen_tcp:socket(),
               sock_monitor :: reference(),
               sock_name :: acceptor_pool:name(),
               peer_name :: acceptor_pool:name(),
               reg_monitor :: reference(),
               mod :: module(),
               call_ref = make_ref() :: reference()}).

%% acceptor api

acceptor_init(SockName, Sock, {Mod, Args, Opts}) ->
    case sregulator:ask(cs_server) of
        {go, _, Pid, _, _} ->
            SockMon = monitor(port, Sock),
            RegMon = monitor(process, Pid),
            Info = #{sock_name => SockName,
                     sock_monitor => SockMon,
                     reg_monitor => RegMon,
                     mod => Mod,
                     args => Args,
                     options => Opts},
            {ok, Info};
        {drop, _} ->
            ignore
    end.

acceptor_continue(PeerName, Sock, #{mod := Mod, args := Args} = Info) ->
    _ = put('$initial_call', {Mod, init, 1}),
    try Mod:init(Args) of
        Result -> init(Result, PeerName, Sock, Info)
    catch
        Result -> init(Result, PeerName, Sock, Info)
    end.

acceptor_terminate(_, _) ->
    ok.

%% gen_statem api

callback_mode() -> handle_event_function.

handle_event(info, {tcp, Sock, Bin}, _, #data{sock=Sock, call_ref=CallRef}) ->
    case cs_packet:decode(Bin) of
        {call, Id, ReqBin} ->
            From = {self(), {CallRef, Id}},
            Req = binary_to_term(ReqBin),
            {keep_state_and_data, [{next_event, {call, From}, Req}]};
        {cast, ReqBin} ->
            Req = binary_to_term(ReqBin),
            {keep_state_and_data, [{next_event, cast, Req}]}
    end;
handle_event(info, {tcp_passive, Sock}, _, #data{sock=Sock}) ->
    case inet:setopts(Sock, [{active, 4}]) of
        ok              -> keep_state_and_data;
        {error, Reason} -> {stop, {tcp_error, Reason}}
    end;
handle_event(info, {tcp_error, Sock, econnreset}, _, #data{sock=Sock}) ->
    {stop, {shutdown, tcp_closed}};
handle_event(info, {tcp_error, Sock, Reason}, _, #data{sock=Sock}) ->
    {stop, {tcp_error, Reason}};
handle_event(info, {tcp_closed, Sock}, _, #data{sock=Sock}) ->
    {stop, {shutdown, tcp_closed}};
handle_event(info, {'DOWN', SockMon, _, _, Reason}, _,
             #data{sock_monitor=SockMon}) ->
    {stop, {shutdown, Reason}};
handle_event(info, {'DOWN', RegMon, _, _, Reason}, _,
             #data{reg_monitor=RegMon}) ->
    {stop, {shutdown, Reason}};
handle_event({call, From}, Req, State, Data) ->
    case handle(handle_call, Req, From, State, Data) of
        {reply, Resp, NState} ->
            {next_state, NState, Data, reply_event(From, Resp, Data)};
        {exception, Error, NState} ->
            {next_state, NState, Data, exception_event(From, Error, Data)}
    end;
handle_event(cast, Req, State, Data) ->
    {noreply, NState} = handle(handle_cast, Req, State, Data),
    {next_state, NState, Data};
handle_event(info, Req, State, Data) ->
    {noreply, NState} = handle(handle_info, Req, State, Data),
    {next_state, NState, Data};
handle_event(internal, {Tag, Id, Resp}, _, #data{sock=Sock}) ->
    IOData = cs_packet:encode(Tag, Id, term_to_binary(Resp)),
    case gen_tcp:send(Sock, IOData) of
        ok               -> keep_state_and_data;
        {error, closed}  -> {stop, {shutdown, tcp_closed}};
        {error, timeout} -> {stop, {shutdown, tcp_timeout}};
        {error, Reason}  -> {stop, {tcp_error, Reason}}
    end.

code_change(OldVsn, State, #data{mod=Mod} = Data, Extra) ->
    try Mod:code_change(OldVsn, State, Extra) of
        {ok, NState} -> {ok, NState, Data}
    catch
        {ok, NState} -> {ok, NState, Data}
    end.

terminate(Reason, State, Data) ->
    handle(terminate, Reason, State, Data).

%% internal

init({ok, State}, PeerName, Sock, Info) ->
    #{sock_name := SockName, sock_monitor := SockMon, reg_monitor := RegMon,
      mod := Mod, options := Opts} = Info,
    Data = #data{sock=Sock, sock_monitor=SockMon, sock_name=SockName,
                 peer_name = PeerName, reg_monitor=RegMon, mod=Mod},
    gen_statem:enter_loop(?MODULE, Opts, State, Data);
init(ignore, _, _, _) ->
    exit(normal);
init({stop, Reason}, _, _, _) ->
    exit(Reason).

handle(Fun, Arg, State, #data{mod=Mod}) ->
    try Mod:Fun(Arg, State) of
        Result -> Result
    catch
        Result -> Result
    end.

handle(Fun, Arg1, Arg2, State, #data{mod=Mod}) ->
    try Mod:Fun(Arg1, Arg2, State) of
        Result -> Result
    catch
        Result -> Result
    end.

reply_event({_, {CallRef, Id}}, Resp, #data{call_ref=CallRef}) ->
    {next_event, internal, {reply, Id, Resp}};
reply_event(From, Resp, _) ->
    {reply, From, Resp}.

exception_event({_, {CallRef, Id}}, Exception, #data{call_ref=CallRef}) ->
    {next_event, internal, {exception, Id, Exception}}.
