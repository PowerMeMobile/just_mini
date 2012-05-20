-module(just_request_acceptor_sup).

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
    {ok, ProcessorSup} =
        supervisor:start_child(Sup,
            {processor, {just_request_processor_sup, start_link, []},
             permanent, infinity, supervisor, [just_request_processor_sup]}),
    {ok, Dedup} =
        supervisor:start_child(Sup,
            {dedup, {just_dedup, start_link, [Gateway#gateway.uuid]},
             permanent, 5000, worker, [just_dedup]}),
    {ok, _} =
        supervisor:start_child(Sup,
            {acceptor,
             {just_request_acceptor, start_link, [Gateway, Dedup, ProcessorSup]},
             transient, 5000, worker, [just_request_acceptor]}),
    {ok, Sup}.

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
