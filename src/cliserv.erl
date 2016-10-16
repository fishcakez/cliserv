-module(cliserv).

%% public api

-export([call/3,
         call/4,
         cast/3]).

%% public api

call(M, F, A) ->
    cs_client:call({M, F, A}).

call(M, F, A, Timeout) ->
    cs_client:call({M, F, A}, Timeout).

cast(M, F, A) ->
    cs_client:cast({M, F, A}).
