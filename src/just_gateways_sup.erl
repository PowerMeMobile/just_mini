-module(just_gateways_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
start_link() ->
    {ok, Sup} = supervisor:start_link(?MODULE, []),
    {ok, GatewaySupSup} =
        supervisor:start_child(Sup,
            {gateway_sup_sup, {just_gateway_sup_sup, start_link, []},
             permanent, infinity, supervisor, [just_gateway_sup_sup]}),
    {ok, _Gateways} =
        supervisor:start_child(Sup,
            {gateways, {just_gateways, start_link, [GatewaySupSup]},
             transient, 5000, worker, [just_gateways]}),
    {ok, Sup}.

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
