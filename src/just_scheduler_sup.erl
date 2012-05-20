-module(just_scheduler_sup).

-include("gateway.hrl").

-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}) -> {ok, pid()}.
start_link(Gateway) ->
    {ok, Sup} = supervisor:start_link(?MODULE, []),
    {ok, WorkerSup} =
        supervisor:start_child(Sup,
            {worker_sup, {just_worker_sup, start_link, [Gateway]},
             permanent, infinity, supervisor, [just_worker_sup]}),
    {ok, _} =
        supervisor:start_child(Sup,
            {scheduler, {just_scheduler, start_link, [Gateway, WorkerSup]},
             transient, 5000, worker, [just_scheduler]}),
    {ok, Sup}.

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
