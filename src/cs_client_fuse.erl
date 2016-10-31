-module(cs_client_fuse).

-behaviour(gen_event).

%% public api

-export([install/0]).
-export([ask/0]).
-export([transport_melt/0]).
-export([service_melt/0]).

%% gen_event api

-export([init/1,
         handle_event/2,
         handle_call/2,
         handle_info/2,
         code_change/3,
         terminate/2]).

%% types

-define(TRANSPORT, cs_client_transport).
-define(SERVICE, cs_client_service).

%% public api

install() ->
    case ensure(?TRANSPORT, client_transport_fuse) of
        ok                 -> ensure(?SERVICE, client_service_fuse);
        {error, _} = Error -> Error
    end.

ask() ->
    case fuse:ask(?TRANSPORT, sync) of
        ok    -> fuse:ask(?SERVICE, sync);
        blown -> blown
    end.

transport_melt() ->
    fuse:melt(?TRANSPORT).

service_melt() ->
    fuse:melt(?SERVICE).

%% gen_event api

init(_) ->
    update(fuse:ask(?TRANSPORT, sync), fuse:ask(?SERVICE, sync)).

handle_event({?TRANSPORT, Transport}, {_, Service}) ->
    update(Transport, Service);
handle_event({?SERVICE, Service}, {Transport, _}) ->
    update(Transport, Service);
handle_event(_, State) ->
    {ok, State}.

handle_call(Req, State) ->
    error(badcall, [Req, State]).

handle_info(_, State) ->
    {ok, State}.

code_change(_, State, _) ->
    {ok, State}.

terminate(_, State) ->
    State.

%% internal

ensure(Fuse, Key) ->
    case fuse:install(Fuse, fuse(Key)) of
        reset -> ok;
        Other -> Other
    end.

fuse(Key) ->
    application:get_env(cliserv, Key, {{standard, 5, 10}, {reset, 30000}}).

update(ok, ok) ->
    update(ok),
    {ok, {ok, ok}};
update(Transport, Service) ->
    update(blown),
    {ok, {Transport, Service}}.

update(Status) ->
    _ = [Pid ! {?MODULE, Status} ||
         {_, Pid, _, _} <- cs_client_pool:which_children(),
         is_pid(Pid)],
    ok.
