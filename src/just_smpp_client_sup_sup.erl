-module(just_smpp_client_sup_sup).

-include("gateway.hrl").

-behaviour(supervisor).

-export([start_link/1]).
-export([start_smpp_client_sup/2]).
-export([init/1]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}) -> {ok, pid()}.
start_link(Gateway) ->
    supervisor:start_link(?MODULE, [Gateway]).

-spec start_smpp_client_sup(pid(), #smpp_connection{}) -> {ok, pid(), pid()}.
start_smpp_client_sup(Sup, Conn) ->
    supervisor:start_child(Sup, [Conn]).

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([Gateway]) ->
    #gateway{uuid = UUID, name = Name, rps = RPS, settings = Settings} = Gateway,
    {ok, {{simple_one_for_one, 0, 1},
          [{smpp_client_sup, {just_smpp_client_sup, start_link,
                              [UUID, Name, RPS, Settings]},
            temporary, brutal_kill, worker, [just_smpp_client_sup]}]}}.
