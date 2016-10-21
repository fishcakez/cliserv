-module(cs_client_pool).

-behaviour(supervisor).

%% public api

-export([start_link/0]).
-export([which_children/0]).

%% supervisor api

-export([init/1]).

%% public api

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

which_children() ->
    supervisor:which_children(?MODULE).

%% supervisor api

init([]) ->
    Half = [half_duplex(Id) || Id <- lists:seq(1, 4)],
    Multi = multiplex(1),
    {ok, {#{strategy => one_for_one, intensity => 3, period => 300},
          [Multi | Half]}}.

%% internal

half_duplex(Id) ->
    #{id => {cs_half_duplex, Id},
      start => {cs_half_duplex, start_link, []},
      type => worker}.

multiplex(Id) ->
    #{id => {cs_multiplex, Id},
      start => {cs_multiplex, start_link, []},
      type => worker}.
