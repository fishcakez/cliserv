-module(cs_server_socket).

-behaviour(gen_server).

%% public api

-export([sockname/0,
         start_link/0]).

%% gen_server api

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

%% public api

sockname() ->
    gen_server:call(?MODULE, sockname).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% gen_server api

init([]) ->
    _ = process_flag(trap_exit, true),
    Port = application:get_env(cliserv, port, 9876),
    IfAddr = application:get_env(cliserv, ifaddr, loopback),
    Opts = [{ifaddr, IfAddr}, {reuseaddr, true}, {packet, 4}, {active, 4},
            {mode, binary}, {send_timeout, 5000}, {send_timeout_close, true}],
    case gen_tcp:listen(Port, Opts) of
        {ok, Sock} ->
            MRef = monitor(port, Sock),
            {ok, _} = cs_server_pool:accept_socket(Sock),
            {ok, {Sock, MRef}};
        {error, Reason} ->
            {stop, {tcp_error, Reason}}
    end.

handle_call(sockname, _, {Sock, _} = State) ->
    {reply, inet:sockname(Sock), State}.

handle_cast(Req, State) ->
    error(badcast, [Req, State]).

handle_info({'DOWN', MRef, _, _, Reason}, {_, MRef} = State) ->
    {stop, Reason, State};
handle_info({'EXIT', _, _}, State) ->
    {noreply, State}.

code_change(_, State, _) ->
    {ok, State}.

terminate(_, {Sock, _}) ->
    gen_tcp:close(Sock).
