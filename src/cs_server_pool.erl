-module(cs_server_pool).

-behaviour(acceptor_pool).

%% public api

-export([accept_socket/1,
         start_link/0]).

%% acceptor_pool api

-export([init/1]).

%% public api

accept_socket(Sock) ->
    acceptor_pool:accept_socket(?MODULE, Sock, 16).

start_link() ->
    acceptor_pool:start_link({local, ?MODULE}, ?MODULE, []).

%% acceptor_pool api

init([]) ->
    {ok, {Mod, Arg, Opts}} = application:get_env(cliserv, server),
    Server = #{id => Mod,
               start => {cs_server, {Mod, Arg, Opts}, Opts},
               type => worker,
               restart => transient,
               modules => [cs_server, Mod]},
    {ok, {#{intensity => 10, period => 300}, [Server]}}.
