-module(cs_multiplex).

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
         send/3,
         recv/3,
         shutdown/3,
         code_change/4,
         terminate/3]).

%% types

-define(BACKOFF, 1000).
-define(TIMEOUT, 3000).
-define(ACTIVE, 8).
-define(SIZE, 4).

-record(data, {broker = undefined :: undefined | {reference(), pid()},
               sender = undefined :: undefined | reference(),
               clients = #{} ::
                #{pos_integer() =>
                  {reference(), pid() | undefined, reference()} | ignore},
               sock :: gen_tcp:socket(),
               counter :: pos_integer(),
               ignores = 0 :: non_neg_integer()}).

%% cs_client api

call(Ref, {Sock, Id, Pid}, BinReq, Timeout) ->
    Packet = cs_packet:encode(call, Id, BinReq),
    case gen_tcp:send(Sock, Packet) of
        ok ->
            MRef = monitor(process, Pid),
            gen_statem:cast(Pid, {sent, Ref}),
            call_await(Ref, Pid, MRef, Timeout);
        {error, Reason} ->
            MRef = monitor(process, Pid),
            gen_statem:cast(Pid, {close, Ref, {inet, Reason}}),
            call_await(Ref, Pid, MRef, infinity)
    end.

cast(Ref, {Sock, Id, Pid}, BinReq) ->
    Packet = cs_packet:encode(cast, undefined, BinReq),
    case gen_tcp:send(Sock, Packet) of
        ok ->
            gen_statem:cast(Pid, {done, Ref, Id});
        {error, Reason} ->
            NReason = {inet, Reason},
            gen_statem:cast(Pid, {close, Ref, NReason}),
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
      #data{broker={MRef, _}} = Data) ->
    go_call(Ref, Pid, Data);
await(info, {MRef, {go, Ref, {cast, Pid}, _, _}},
      #data{broker={MRef, _}} = Data) ->
    go_cast(Ref, Pid, Data);
await(info, {MRef, {drop, _}}, #data{broker={MRef, _}} = Data) ->
    demonitor(MRef, [flush]),
    shutdown(Data#data{broker=undefined});
await(info, {'DOWN', MRef, _, _, _}, #data{broker={MRef, _}} = Data) ->
    shutdown(Data#data{broker=undefined});
await(info, {tcp, Sock, Bin}, #data{sock=Sock} = Data) ->
    await_decode(Bin, Data);
await(info, {tcp_error, Sock, Reason}, #data{sock=Sock} = Data) ->
    cancel({inet, Reason}, Data);
await(info, {tcp_closed, Sock}, #data{sock=Sock} = Data) ->
    cancel({inet, closed}, Data);
await(Type, Event, Data) ->
    handle_event(Type, Event, await, Data).

send(info, {tcp, Sock, Bin}, #data{sock=Sock} = Data) ->
    send_decode(Bin, Data);
send(info, {tcp_error, Sock, Reason}, #data{sock=Sock} = Data) ->
    close({inet, Reason}, Data);
send(info, {tcp_closed, Sock}, #data{sock=Sock} = Data) ->
    close({inet, closed}, Data);
send(cast, {sent, Ref}, #data{sender=Ref} = Data) when is_reference(Ref) ->
    send_sent(Data);
send(cast, {close, Ref, Reason}, #data{sender=Ref} = Data)
  when is_reference(Ref) ->
    close(Reason, Data);
send(cast, {done, Ref, Id}, #data{sender=Ref} = Data) when is_reference(Ref) ->
    done(Id, Data);
send(Type, Event, Data) ->
    handle_event(Type, Event, send, Data).

recv(info, {tcp, Sock, Bin}, #data{sock=Sock} = Data) ->
    recv_decode(Bin, Data);
recv(info, {tcp_error, Sock, Reason}, #data{sock=Sock} = Data) ->
    close({inet, Reason}, Data);
recv(info, {tcp_closed, Sock}, #data{sock=Sock} = Data) ->
    close({inet, closed}, Data);
recv(Type, Event, Data) ->
    handle_event(Type, Event, recv, Data).

shutdown(info, {tcp, Sock, Bin}, #data{sock=Sock} = Data) ->
    shutdown_decode(Bin, Data);
shutdown(info, {tcp_error, Sock, Reason}, #data{sock=Sock} = Data) ->
    close({inet, Reason}, Data);
shutdown(info, {tcp_closed, Sock}, #data{sock=Sock} = Data) ->
    close({inet, closed}, Data);
shutdown(cast, {sent, Ref}, #data{sender=Ref} = Data) when is_reference(Ref) ->
    shutdown_sent(Data);
shutdown(cast, {close, Ref, Reason}, #data{sender=Ref} = Data)
  when is_reference(Ref) ->
    close(Reason, Data);
shutdown(Type, Event, Data) ->
    handle_event(Type, Event, shutdown, Data).

code_change(_, State, Data, _) ->
    {ok, State, Data}.

terminate(_, _, _) ->
    ok.

%% internal

handle_event(info, {tcp_passive, Sock}, _, #data{sock=Sock} = Data) ->
    case inet:setopts(Sock, [{active, ?ACTIVE}]) of
        ok              -> keep_state_and_data;
        {error, einval} -> close({inet, closed}, Data);
        {error, Reason} -> close({inet, Reason}, Data)
    end;
handle_event(info, {'DOWN', MRef, _, _, _}, State, Data) ->
    down(MRef, State, Data);
handle_event(cast, {close, _, _}, _, _) ->
    keep_state_and_data;
handle_event(cast, {sent, _}, _, _) ->
    keep_state_and_data;
handle_event(cast, {done, _, _}, _, _) ->
    keep_state_and_data;
handle_event(cast, {timeout, Ref}, State, #data{sender=Sender} = Data)
  when Sender =/= Ref ->
    timeout(Ref, State, Data).

connect(Addr, Port) ->
    Opts = [{packet, 4}, {active, ?ACTIVE}, {mode, binary},
            {send_timeout, 5000}, {send_timeout_close, true},
            {show_econnreset, true}, {exit_on_close, false}],
    case gen_tcp:connect(Addr, Port, Opts, ?TIMEOUT) of
        {ok, Sock} ->
            {await, MRef, Pid} = async_ask_r(Sock, 1),
            Data = #data{broker={MRef, Pid}, sock=Sock, counter=1},
            {next_state, await, Data};
        {error, Reason} ->
            backoff({inet, Reason})
    end.

async_ask_r(Sock, Id) ->
    cs_client:async_ask_r(?MODULE, {Sock, Id, self()}).

backoff(Reason) ->
    error_logger:error_msg("cliserv ~p ~p backing off: ~ts~n",
                           [?MODULE, self(), cs_client:format_error(Reason)]),
    backoff().

backoff() ->
    Backoff = ?BACKOFF div 2 + rand:uniform(?BACKOFF),
    {keep_state, erlang:start_timer(Backoff, self(), connect)}.

go_call(Ref, Pid,
   #data{broker={BrokerMRef, _}, clients=Clients, counter=Counter} = Data) ->
    demonitor(BrokerMRef, [flush]),
    ClientMRef = monitor(process, Pid),
    NClients = Clients#{Counter => {Ref, Pid, ClientMRef}},
    NData = Data#data{broker=undefined, clients=NClients, counter=Counter+1,
                      sender=Ref},
    {next_state, send, NData}.

go_cast(Ref, Pid, #data{broker={BrokerMRef, _}, clients=Clients,
                        counter=Counter} = Data) ->
    demonitor(BrokerMRef, [flush]),
    ClientMRef = monitor(process, Pid),
    NClients = Clients#{Counter => {Ref, undefined, ClientMRef}},
    NData = Data#data{broker=undefined, clients=NClients, sender=Ref},
    {next_state, send, NData}.

await_decode(Bin, Data) ->
    {keep_state, decode(Bin, Data)}.

send_decode(Bin, Data) ->
    case decode(Bin, Data) of
        #data{sender=undefined, sock=Sock, counter=Counter} = NData ->
            {await, MRef, Pid} = async_ask_r(Sock, Counter),
            {next_state, await, NData#data{broker={MRef, Pid}}};
        NData ->
            {keep_state, NData}
    end.

recv_decode(Bin, #data{clients=Clients} = Data)
  when map_size(Clients) == ?SIZE ->
    #data{sock=Sock, counter=Counter} = NData = decode(Bin, Data),
    {await, MRef, Pid} = async_ask_r(Sock, Counter),
    {next_state, await, NData#data{broker={MRef, Pid}}}.

shutdown_decode(Bin, Data) ->
    case decode(Bin, Data) of
        #data{clients=Clients, ignores=Ignores} = NData
          when map_size(Clients) == Ignores ->
            close(shutdown, NData);
        NData ->
            {keep_state, NData}
    end.

decode(Bin, #data{clients=Clients, ignores=Ignores, sender=Sender} = Data) ->
    {reply, Id, BinResp} = cs_packet:decode(Bin),
    case maps:take(Id, Clients) of
        {{Sender, Pid, MRef}, NClients} when is_pid(Pid) ->
            client_reply(Pid, Sender, BinResp),
            demonitor(MRef, [flush]),
            Data#data{clients=NClients, sender=undefined};
        {{Ref, Pid, MRef}, NClients} when is_pid(Pid) ->
            client_reply(Pid, Ref, BinResp),
            demonitor(MRef, [flush]),
            Data#data{clients=NClients};
        {ignore, NClients} ->
            Data#data{clients=NClients, ignores=Ignores-1}
    end.

send_sent(#data{clients=Clients, sock=Sock, counter=Counter} = Data)
  when map_size(Clients) < ?SIZE ->
    {await, MRef, Pid} = async_ask_r(Sock, Counter),
    {next_state, await, Data#data{sender=undefined, broker={MRef, Pid}}};
send_sent(Data) ->
    {next_state, recv, Data#data{sender=undefined}}.

done(Id, #data{clients=Clients, sock=Sock, counter=Counter} = Data) ->
    {{_, _, MRef}, NClients} = maps:take(Id, Clients),
    demonitor(MRef, [flush]),
    {await, NMRef, Pid} = async_ask_r(Sock, Counter),
    {next_state, await, Data#data{broker={NMRef, Pid}, clients=NClients}}.

shutdown_sent(#data{sock=Sock} = Data) ->
    NData = Data#data{sender=undefined},
    case gen_tcp:shutdown(Sock, write) of
        ok               -> {keep_state, NData};
        {error, NReason} -> close({inet, NReason}, NData)
    end.

down(MRef, State, #data{sender=Ref, clients=Clients, ignores=Ignores} = Data) ->
    case find(MRef, 3, Data) of
        {Id, {Ref, undefined, _}} ->
            NData = Data#data{clients=maps:remove(Id, Clients),
                              sender=undefined},
            shutdown(NData);
        {Id, {Ref, _, _}} ->
            NData = Data#data{clients=Clients#{Id := ignore}, ignores=Ignores+1,
                              sender=undefined},
            shutdown(NData);
        {Id, _} ->
            ignore(Id, State, Data)
    end.

timeout(Ref, State, Data) ->
    case find(Ref, 1, Data) of
        {Id, {Ref, Pid, MRef}} when is_pid(Pid) ->
            Reason = {inet, timeout},
            client_error(Pid, Ref, Reason),
            demonitor(MRef, [flush]),
            ignore(Id, State, Data);
        error ->
            keep_state_and_data
    end.

find(Ref, Elem, #data{clients=Clients}) ->
    Filter = fun({_, Info}) when element(Elem, Info) == Ref -> false;
                (_)                                         -> true
             end,
    case lists:dropwhile(Filter, maps:to_list(Clients)) of
        [Client | _] -> Client;
        []           -> error
    end.

ignore(Id, State, #data{clients=Clients, ignores=Ignores} = Data) ->
    NIgnores = Ignores+1,
    NClients = Clients#{Id := ignore},
    NData = Data#data{clients=NClients, ignores=NIgnores},
    if
        map_size(NClients) == NIgnores, State == await ->
            ignore_cancel(NData);
        map_size(NClients) == NIgnores ->
            close(shutdown, NData);
        true ->
            {keep_state, NData}
    end.

ignore_cancel(#data{broker={MRef, Pid}} = Data) ->
    case cs_client:cancel(Pid, MRef) of
        1 ->
            demonitor(MRef, [flush]),
            close(shutdown, Data#data{broker=undefined});
        false ->
            ignore_await(Data)
    end.

ignore_await(#data{broker={MRef, _}} = Data) ->
    case sbroker:await(MRef, 0) of
        {go, Ref, {call, Pid}, _, _} ->
            ignore_go(Ref, Pid, Data);
        {drop, _} ->
            demonitor(MRef, [flush]),
            close(shutdown, Data#data{broker=undefined})
    end.

ignore_go(Ref, Pid, #data{broker={BrokerMRef, _}, clients=Clients,
                          counter=Counter} = Data) ->
    demonitor(BrokerMRef, [flush]),
    ClientMRef = monitor(process, Pid),
    NClients = Clients#{Counter => {Ref, Pid, ClientMRef}},
    NData = Data#data{broker=undefined, clients=NClients, counter=Counter+1,
                      sender=Ref},
    {next_state, shutdown, NData}.

cancel(Reason, #data{broker={MRef, Pid}} = Data) ->
    case cs_client:cancel(Pid, MRef) of
        1 ->
            demonitor(MRef, [flush]),
            close(Reason, Data#data{broker=undefined});
        false ->
            cancel_await(Reason, Data)
    end.

cancel_await(Reason, #data{broker={MRef, _}} = Data) ->
    NData = Data#data{broker=undefined},
    case sbroker:await(MRef, 0) of
        {go, Ref, {call, Pid}, _, _} ->
            client_error(Pid, Ref, Reason),
            demonitor(MRef, [flush]),
            close(Reason, NData);
        {drop, _} ->
            demonitor(MRef, [flush]),
            close(Reason, NData)
    end.

close(Reason, #data{clients=Clients, broker=undefined, sock=Sock} = Data) ->
    report_close(Reason),
    _ = [client_error(Pid, Ref, Reason) ||
         {_, {Ref, Pid, MRef}} <- maps:to_list(Clients),
         demonitor(MRef, [flush])],
    gen_tcp:close(Sock),
    flush(Data),
    {next_state, closed, undefined, {next_event, internal, connect}}.

report_close(shutdown) ->
    ok;
report_close(Reason) ->
    error_logger:error_msg("~p ~p closing socket: ~ts~n",
                           [?MODULE, self(), cs_client:format_error(Reason)]).

flush(#data{sock=Sock} = Data) ->
    receive
        {tcp, Sock, _}          -> flush(Data);
        {tcp_error, Sock, _}    -> flush(Data);
        {tcp_closed, Sock}      -> flush(Data);
        {tcp_passive, Sock}     -> ok
    after
        0                       -> ok
    end.

shutdown(#data{clients=Clients, ignores=Ignores} = Data)
  when map_size(Clients) == Ignores ->
    close(shutdown, Data);
shutdown(#data{sock=Sock} = Data) ->
    case gen_tcp:shutdown(Sock, write) of
        ok              -> {next_state, shutdown, Data};
        {error, Reason} -> close({inet, Reason}, Data)
    end.

call_await(Ref, Pid, MRef, Timeout) ->
    receive
        {reply, Ref, BinReq} ->
            demonitor(MRef, [flush]),
            {ok, BinReq};
        {error, Ref, Reason} ->
            demonitor(MRef, [flush]),
            {error, Reason};
        {'DOWN', MRef, _, _, Reason} ->
            {exit, Reason}
    after
        Timeout ->
            gen_statem:cast(Pid, {timeout, Ref}),
            call_await(Ref, Pid, MRef, infinity)
    end.

client_reply(Pid, Ref, BinReq) ->
    _ = Pid ! {reply, Ref, BinReq},
    ok.

client_error(Pid, Ref, Reason) ->
    _ = Pid ! {error, Ref, Reason},
    ok.
