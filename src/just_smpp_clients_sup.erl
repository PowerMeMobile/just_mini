-module(just_smpp_clients_sup).

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
    {ok, ClientSupSup} =
        supervisor:start_child(Sup,
            {smpp_client_sup_sup,
             {just_smpp_client_sup_sup, start_link, [Gateway]},
             permanent, infinity, supervisor, [just_smpp_client_sup_sup]}),
    {ok, _} =
        supervisor:start_child(Sup,
            {smpp_clients,
             {just_smpp_clients, start_link, [Gateway, ClientSupSup]},
             transient, 5000, worker, [just_smpp_clients]}),
    {ok, Sup}.

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
