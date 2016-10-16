-module(cliserv_sup).

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
    Server = #{id => cs_server_sup,
               start => {cs_server_sup, start_link, []},
               type => supervisor},
    Client = #{id => cs_client_sup,
               start => {cs_client_sup, start_link, []},
               type => supervisor},
    {ok, {#{strategy => one_for_one, intensity => 3, period => 600},
          [Server, Client]}}.
