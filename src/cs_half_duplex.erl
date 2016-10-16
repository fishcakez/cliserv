-module(cs_half_duplex).

-behaviour(cs_client).
-behaviour(gen_statem).

%% cs_client api

-export([call/4,
         cast/3]).

%% public api

-export([start_link/0]).

%% gen_statem api

-export([init/1,
         callback_mode/0,
         closed/3,
         await/3,
         recv/3,
         norecv/3,
         code_change/4,
         terminate/3]).

%% types

-define(BACKOFF, 1000).
-define(TIMEOUT, 3000).

-record(data, {monitor :: {reference(), pid() | reference()},
               sock :: gen_tcp:socket(),
               counter :: pos_integer()}).

%% cs_client api

call(Ref, {Sock, Id, Pid}, BinReq, Timeout) ->
    Packet = cs_packet:encode(call, Id, BinReq),
    case gen_tcp:send(Sock, Packet) of
        ok              -> call_await(Ref, Pid, Id, Sock, Timeout);
        {error, Reason} -> call_error(Ref, Pid, {inet, Reason})
    end.

cast(Ref, {Sock, _, Pid}, BinReq) ->
    Packet = cs_packet:encode(cast, undefined, BinReq),
    case gen_tcp:send(Sock, Packet) of
        ok ->
            done(Pid, Ref);
        {error, Reason} ->
            NReason = {inet, Reason},
            close(Pid, Ref, NReason),
            {error, NReason}
    end.

%% public api

start_link() ->
    gen_statem:start_link(?MODULE, [], []).

%% gen_statem api

init([]) ->
    {ok, closed, undefined, {next_event, internal, connect}}.

callback_mode() ->
    state_functions.

closed(internal, connect, undefined) ->
    try cs_server_socket:sockname() of
        {ok, {local, _} = Local} -> connect(Local, 0);
        {ok, {Ip, Port}}         -> connect(Ip, Port);
        {error, Reason}          -> backoff({inet, Reason})
    catch
        exit:{noproc, _}         -> backoff()
    end;
closed(info, {timeout, TRef, connect}, TRef) ->
    {keep_state, undefined, {next_event, internal, connect}};
closed(Type, Event, Data) ->
    handle_event(Type, Event, closed, Data).

await(info, {MRef, {go, Ref, {call, Pid}, _, _}},
      #data{monitor={MRef, _}} = Data) ->
    active_recv(Ref, Pid, Data);
await(info, {MRef, {go, Ref, {cast, Pid}, _, _}},
      #data{monitor={MRef, _}} = Data) ->
    active_norecv(Ref, Pid, Data);
await(info, {MRef, {drop, _}}, #data{monitor={MRef, _}} = Data) ->
    close(shutdown, Data);
await(info, {tcp_error, Sock, Reason}, #data{sock=Sock} = Data) ->
    cancel({inet, Reason}, Data);
await(info, {tcp_closed, Sock}, #data{sock=Sock} = Data) ->
    cancel({inet, closed}, Data);
await(Type, Event, Data) ->
    handle_event(Type, Event, await, Data).

recv(cast, {done, MRef}, #data{monitor={MRef, _}, counter=Counter} = Data) ->
    passive_ask(Data#data{counter=Counter+1});
recv(cast, {close, MRef, Reason}, #data{monitor={MRef, _}} = Data) ->
    close(Reason, Data);
recv(Type, Event, Data) ->
    handle_event(Type, Event, recv, Data).

norecv(info, {tcp_error, Sock, Reason}, #data{sock=Sock} = Data) ->
    close({inet, Reason}, Data);
norecv(info, {tcp_closed, Sock}, #data{sock=Sock} = Data) ->
    close({inet, closed}, Data);
norecv(cast, {done, Ref}, #data{monitor={_, Ref}} = Data) ->
    active_ask(Data);
norecv(cast, {close, Ref, Reason}, #data{monitor={_, Ref}} = Data) ->
    close(Reason, Data);
norecv(Type, Event, Data) ->
    handle_event(Type, Event, norecv, Data).

code_change(_, State, Data, _) ->
    {ok, State, Data}.

terminate(_, _, _) ->
    ok.

%% internal

handle_event(info, {'DOWN', MRef, _, _, _}, _,
             #data{monitor={MRef, _}} = Data) ->
    close(shutdown, Data).

connect(Addr, Port) ->
    Opts = [{packet, 4}, {active, once}, {mode, binary}, {send_timeout, 5000},
            {send_timeout_close, true}, {show_econnreset, true}],
    case gen_tcp:connect(Addr, Port, Opts, ?TIMEOUT) of
        {ok, Sock} ->
            Info = {Sock, 1, self()},
            {await, MRef, Pid} = cs_client:async_ask_r(?MODULE, Info),
            Data = #data{monitor={MRef, Pid}, sock=Sock, counter=1},
            {next_state, await, Data};
        {error, Reason} ->
            backoff({inet, Reason})
    end.

backoff(Reason) ->
    error_logger:error_msg("cliserv ~p ~p backing off: ~ts~n",
                           [?MODULE, self(), cs_client:format_error(Reason)]),
    backoff().

backoff() ->
    Backoff = ?BACKOFF div 2 + rand:uniform(?BACKOFF),
    {keep_state, erlang:start_timer(Backoff, self(), connect)}.

passive_ask(#data{monitor={MRef, _}, sock=Sock, counter=Counter} = Data) ->
    demonitor(MRef, [flush]),
    case cs_client:dynamic_ask_r(?MODULE, {Sock, Counter, self()}) of
        {go, Ref, {call, Pid}, _, _} -> passive_recv(Ref, Pid, Data);
        {go, Ref, {cast, Pid}, _, _} -> passive_norecv(Ref, Pid, Data);
        {await, NMRef, Pid}          -> activate(NMRef, Pid, Data)
    end.

active_ask(#data{monitor={MRef, _}, sock=Sock, counter=Counter} = Data) ->
    demonitor(MRef, [flush]),
    Info = {Sock, Counter, self()},
    {await, NMRef, Pid} = cs_client:async_ask_r(?MODULE, Info),
    {next_state, await, Data#data{monitor={NMRef, Pid}}}.

passive_recv(Ref, Pid, #data{monitor={MRef, _}} = Data) ->
    NMRef = client_recv(Pid, Ref),
    demonitor(MRef, [flush]),
    {next_state, recv, Data#data{monitor={NMRef, Pid}}}.

active_recv(Ref, Pid, Data) when is_reference(Ref) ->
    case pacify(Data) of
        recv ->
            passive_recv(Ref, Pid, Data);
        {Next, Bin} when is_function(Next, 1) ->
            active_decode(Ref, Pid, Bin, Data),
            Next(Data);
        {error, Reason} ->
            client_error(Pid, Ref, Reason),
            close(Reason, Data)
    end.

active_decode(Ref, Pid, Bin, #data{counter=Counter}) ->
    {reply, Counter, BinResp} = cs_packet:decode(Bin),
    client_reply(Pid, Ref, BinResp).

pacify(#data{sock=Sock} = Data) ->
    case inet:setopts(Sock, [{active, false}]) of
        ok              -> flush(recv, Data);
        {error, einval} -> flush({error, {inet, closed}}, Data);
        {error, Reason} -> flush({error, {inet, Reason}}, Data)
    end.

flush(Data) ->
    _ = flush(recv, Data),
    ok.

flush(Result, #data{sock=Sock}) ->
    receive
        {tcp, Sock, Bin}          -> {next_fun(Result), Bin};
        {tcp_error, Sock, Reason} -> {error, {inet, Reason}};
        {tcp_closed, Sock}        -> {error, {inet, closed}}
    after
        0                          -> Result
    end.

next_fun(recv)            -> fun passive_ask/1;
next_fun({error, Reason}) -> fun(Data) -> close(Reason, Data) end.

passive_norecv(Ref, Pid, #data{sock=Sock} = Data) ->
    case inet:setopts(Sock, [{active, once}]) of
        ok              -> active_norecv(Ref, Pid, Data);
        {error, einval} -> cancel({inet, closed}, Data);
        {error, Reason} -> cancel({inet, Reason}, Data)
    end.

active_norecv(Ref, Pid, #data{monitor={MRef, _}} = Data) ->
    demonitor(MRef, [flush]),
    NMRef = monitor(process, Pid),
    {next_state, norecv, Data#data{monitor={NMRef, Ref}}}.

activate(MRef, Pid, #data{sock=Sock} = Data) ->
    NData = Data#data{monitor={MRef, Pid}},
    case inet:setopts(Sock, [{active, once}]) of
        ok              -> {next_state, await, NData};
        {error, einval} -> cancel({inet, closed}, NData);
        {error, Reason} -> cancel({inet, Reason}, NData)
    end.

cancel(Reason, #data{monitor={MRef, Pid}} = Data) ->
    case cs_client:cancel(Pid, MRef) of
        1     -> close(Reason, Data);
        false -> cancel_await(Reason, Data)
    end.

cancel_await(Reason, #data{monitor={MRef, _}} = Data) ->
    case sbroker:await(MRef, 0) of
        {go, Ref, {call, Pid}, _, _} ->
            client_error(Pid, Ref, Reason),
            close(Reason, Data);
        {go, _, {cast, _}, _, _} ->
            close(Reason, Data);
        {drop, _} ->
            close(Reason, Data)
    end.

close(Reason, #data{monitor={MRef, _}, sock=Sock} = Data) ->
    report_close(Reason),
    gen_tcp:close(Sock),
    flush(Data),
    demonitor(MRef, [flush]),
    {next_state, closed, undefined, {next_event, internal, connect}}.

report_close(shutdown) ->
    ok;
report_close(Reason) ->
    error_logger:error_msg("~p ~p closing socket: ~ts~n",
                           [?MODULE, self(), cs_client:format_error(Reason)]).

call_await(Ref, Pid, Id, Sock, Timeout) ->
    MRef = monitor(process, Pid),
    receive
        {recv, Ref, NRef} ->
            demonitor(MRef, [flush]),
            call_recv(NRef, Pid, Id, gen_tcp:recv(Sock, 0, Timeout));
        {reply, Ref, BinResp} ->
            demonitor(MRef, [flush]),
            {ok, BinResp};
        {error, Ref, Reason} ->
            demonitor(MRef, [flush]),
            {error, Reason};
        {'DOWN', MRef, _, _, Reason} ->
            {exit, Reason}
    end.

call_recv(Ref, Pid, Id, {ok, Data}) ->
    case cs_packet:decode(Data) of
        {reply, Id, BinResp} ->
            done(Pid, Ref),
            {ok, BinResp};
        {error, Reason} ->
            NReason = {packet, Reason},
            close(Pid, Ref, NReason),
            {error, NReason};
        Packet ->
            NReason = {packet, Packet},
            close(Pid, Ref, {packet, Packet}),
            {error, NReason}
    end;
call_recv(Ref, Pid, _, {error, Reason}) ->
    NReason = {inet, Reason},
    close(Pid, Ref, NReason),
    {error, NReason}.

call_error(Ref, Pid, Reason) ->
    MRef = monitor(process, Pid),
    receive
        {recv, Ref, NRef} ->
            demonitor(MRef, [flush]),
            close(Pid, NRef, Reason),
            {error, Reason};
        {reply, Ref, BinResp} ->
            demonitor(MRef, [flush]),
            {ok, binary_to_term(BinResp)};
        {error, Ref, NReason} ->
            demonitor(MRef, [flush]),
            {error, NReason};
        {'DOWN', MRef, _, _, Reason} ->
            {exit, Reason}
    end.

done(Pid, Ref) ->
    gen_statem:cast(Pid, {done, Ref}).

close(Pid, Ref, Reason) ->
    gen_statem:cast(Pid, {close, Ref, Reason}).

client_recv(Pid, Ref) ->
    MRef = monitor(process, Pid),
    _ = Pid ! {recv, Ref, MRef},
    MRef.

client_error(Pid, Ref, Reason) ->
    _ = Pid ! {error, Ref, Reason},
    ok.

client_reply(Pid, Ref, Resp) ->
    _ = Pid ! {reply, Ref, Resp},
    ok.
