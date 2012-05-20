-module(just_worker_sup).

-include("gateway.hrl").

-behaviour(supervisor).

-export([start_link/1]).
-export([start_worker/1]).
-export([init/1]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}) -> {ok, pid()}.
start_link(Gateway) ->
    supervisor:start_link(?MODULE, [Gateway]).

-spec start_worker(pid()) -> pid().
start_worker(Sup) ->
    {ok, Worker} = supervisor:start_child(Sup, []),
    Worker.

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([Gateway]) ->
    #gateway{uuid = UUID, name = Name, settings = Settings} = Gateway,
    {ok, {{simple_one_for_one, 0, 1},
          [{worker, {just_worker, start_link, [UUID, Name, Settings]},
            temporary, brutal_kill, worker, [just_worker]}]}}.
