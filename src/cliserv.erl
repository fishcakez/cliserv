-module(cliserv).

%% public api

-export([apply/3]).

%% public api

apply(M, F, A) ->
    cs_client:call({M, F, A}).
