-module(cs_client_sup).

-behaviour(supervisor).

%% public api

-export([start_link/0]).

%% supervisor api

-export([init/1]).

%% public api

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% supervisor api

init([]) ->
    {ok, {#{strategy => rest_for_one, intensity => 1, period => 300},
          [pool(), fuse()]}}.

%% internal

pool() ->
    #{id => cs_client_pool,
      start => {cs_client_pool, start_link, []},
      type => supervisor}.

fuse() ->
    #{id => {cs_client_watcher, cs_client_fuse},
      start => {cs_client_watcher, start_link, [fuse_event, cs_client_fuse]},
      type => worker}.
