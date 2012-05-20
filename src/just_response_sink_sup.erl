-module(just_response_sink_sup).

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
    {ok, PublisherSup} =
        supervisor:start_child(Sup,
            {publisher_sup, {just_publisher_sup, start_link, []},
             permanent, infinity, supervisor, [just_publisher_sup]}),
    {ok, _} =
        supervisor:start_child(Sup,
            {response_sink,
             {just_response_sink, start_link, [Gateway, PublisherSup]},
             transient, 5000, worker, [just_response_sink]}),
    {ok, Sup}.

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
