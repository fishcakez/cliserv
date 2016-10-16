-module(cliserv).

%% public api

-export([apply/3,
         cast/3]).

%% public api

apply(M, F, A) ->
    cs_client:call({M, F, A}).

cast(M, F, A) ->
    cs_client:cast({M, F, A}).
