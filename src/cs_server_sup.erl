-module(cs_server_sup).

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
    Pool = #{id => cs_server_pool,
             start => {cs_server_pool, start_link, []},
             type => supervisor},
    Socket = #{id => cs_server_socket,
               start => {cs_server_socket, start_link, []},
               type => worker},
    {ok, {#{strategy => rest_for_one, intensity => 3, period => 300},
          [Pool, Socket]}}.
