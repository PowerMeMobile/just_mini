-module(just_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 10, 60}, [
			{just_throughput, {just_throughput, start_link, []},
			permanent, 5000, worker, [just_throughput]},

			{customers_sup, {just_customers_sup, start_link, []},
            permanent, infinity, supervisor, [just_customers_sup]},

           {gateways_sup, {just_gateways_sup, start_link, []},
            permanent, infinity, supervisor, [just_gateways_sup]},

			{just_amqp_control, {just_amqp_control, start_link, []},
			permanent, 5000, worker, [just_amqp_control]}
	]}}.
