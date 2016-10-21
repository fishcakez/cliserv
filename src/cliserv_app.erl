-module(cliserv_app).

-behaviour(application).

%% application api

-export([start/2,
         stop/1]).

%% application api

start(_StartType, _StartArgs) ->
    ok = cs_client_fuse:install(),
    cliserv_sup:start_link().

stop(_State) ->
    ok.
