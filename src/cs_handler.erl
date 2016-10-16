-module(cs_handler).

-behaviour(cs_server).

%% cs_server api

-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

%% cs_server api

init(_) ->
    {ok, undefined}.

handle_call({M, F, A}, _, State) ->
    {reply, apply(M, F, A), State}.

handle_cast({M, F, A}, State) ->
    apply(M, F, A),
    {noreply, State}.

handle_info(Msg, State) ->
    error_logger:error_msg("cs_handler ~p received unexpected message: ~p",
                           [self(), Msg]),
    {noreply, State}.

code_change(_, State, _) ->
    {ok, State}.

terminate(_, _) ->
    ok.
