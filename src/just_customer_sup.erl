-module(just_customer_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([start_customer/3]).
-export([init/1]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link(?MODULE, []).

-spec start_customer(pid(), binary(), pos_integer()) -> pid().
start_customer(Sup, UUID, RPS) ->
    {ok, Customer} = supervisor:start_child(Sup, [UUID, RPS]),
    Customer.

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([]) ->
     {ok, {{simple_one_for_one, 0, 1},
           [{customer, {just_customer, start_link, []},
             temporary, brutal_kill, worker, [just_customer]}]}}.
