-module(just_publisher_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([start_publisher/5]).
-export([init/1]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link(?MODULE, []).

-spec start_publisher(pid(), binary(), string(), message | receipt | response,
                      pid()) -> pid().
start_publisher(Sup, UUID, Name, Type, Chan) ->
    {ok, Pid} = supervisor:start_child(Sup, [UUID, Name, Type, Chan]),
    Pid.

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([]) ->
     {ok, {{simple_one_for_one, 0, 1},
           [{publisher, {just_publisher, start_link, []},
             temporary, brutal_kill, worker, [just_publisher]}]}}.
