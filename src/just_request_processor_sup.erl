-module(just_request_processor_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([start_processor/2]).
-export([init/1]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link(?MODULE, []).

-spec start_processor(pid(), binary()) -> pid().
start_processor(Sup, UUID) ->
    {ok, Pid} = supervisor:start_child(Sup, [UUID]),
    Pid.

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([]) ->
     {ok, {{simple_one_for_one, 0, 1},
           [{processor, {just_request_processor, start_link, []},
             temporary, brutal_kill, worker, [just_request_processor]}]}}.
