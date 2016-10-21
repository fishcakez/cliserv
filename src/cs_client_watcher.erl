-module(cs_client_watcher).

%% public api

-export([start_link/2]).

%% gen_server api

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

%% public api

start_link(Manager, Handler) ->
    gen_server:start_link({local, Handler}, ?MODULE, {Manager, Handler}, []).

%% gen_server api

init({Manager, Handler} = State) ->
    ok = gen_event:add_sup_handler(Manager, Handler, []),
    {ok, State}.

handle_call(Req, From, State) ->
    error(badcall, [Req, From, State]).

handle_cast(Req, State) ->
    error(badcast, [Req, State]).

handle_info({gen_event_EXIT, Handler, Reason}, {_, Handler} = State) ->
    {stop, Reason, State}.

code_change(_, State, _) ->
    {ok, State}.

terminate(Reason, {Manager, Handler}) ->
    gen_event:delete_handler(Manager, Handler, Reason).
