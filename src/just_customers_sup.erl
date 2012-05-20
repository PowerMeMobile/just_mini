-module(just_customers_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
start_link() ->
    {ok, Sup} = supervisor:start_link(?MODULE, []),
    {ok, CustomerSup} =
        supervisor:start_child(Sup,
            {customer_sup, {just_customer_sup, start_link, []},
             permanent, infinity, supervisor, [just_customer_sup]}),
    {ok, _} =
        supervisor:start_child(Sup,
            {customers, {just_customers, start_link, [CustomerSup]},
             permanent, 5000, worker, [just_customers]}),
    {ok, Sup}.

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 10, 10}, []}}.
