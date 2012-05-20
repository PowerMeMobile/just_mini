-module(just_gateway_sup_sup).

-include("gateway.hrl").

-behaviour(supervisor).

-export([start_link/0]).
-export([start_gateway_sup/2, terminate_gateway_sup/2, delete_gateway_sup/2]).
-export([init/1]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link(?MODULE, []).

-spec start_gateway_sup(pid(), #gateway{}) -> {ok, pid()}.
start_gateway_sup(Sup, Gateway) ->
    supervisor:start_child(Sup,
        {child_id(Gateway), {just_gateway_sup, start_link, [Gateway]},
         permanent, infinity, supervisor, [just_gateway_sup]}).

-spec terminate_gateway_sup(pid(), #gateway{}) -> ok | {error, not_found}.
terminate_gateway_sup(Sup, Gateway) ->
    supervisor:terminate_child(Sup, child_id(Gateway)).

-spec delete_gateway_sup(pid(), #gateway{}) ->
    ok | {error, running | restarting | not_found}.
delete_gateway_sup(Sup, Gateway) ->
    supervisor:delete_child(Sup, child_id(Gateway)).

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_one, 5, 30}, []}}.

%% -------------------------------------------------------------------------
%% private functions
%% -------------------------------------------------------------------------

child_id(Gateway) ->
    binary_to_list(uuid:unparse(Gateway#gateway.uuid)).
