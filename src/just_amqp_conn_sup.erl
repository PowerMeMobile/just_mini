-module(just_amqp_conn_sup).

-include("gateway.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(supervisor).

-export([start_link/1]).
-export([start_amqp_connection_sup/1]).
-export([init/1]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}) -> {ok, pid(), pid()}.
start_link(Gateway) ->
    {ok, Sup} = supervisor:start_link(?MODULE, []),
    {ok, _ConnSup, Conn} = start_amqp_connection_sup(Sup),
    {ok, JustConn} =
        supervisor:start_child(Sup,
            {just_amqp_conn, {just_amqp_conn, start_link, [Gateway, Sup, Conn]},
             transient, 5000, worker, [just_amqp_conn]}),
    {ok, Sup, JustConn}.

-spec start_amqp_connection_sup(pid()) -> {ok, pid(), pid()}.
start_amqp_connection_sup(Sup) ->
    % terminate first to be sure.
    supervisor:terminate_child(Sup, amqp_conn_sup),
    Params = #amqp_params_network{virtual_host = just_app:get_env(amqp_vhost),
                                  username = just_app:get_env(amqp_username),
                                  password = just_app:get_env(amqp_password),
                                  host = just_app:get_env(amqp_host),
                                  port = just_app:get_env(amqp_port)},
    supervisor:start_child(Sup,
        {amqp_conn_sup, {amqp_connection_sup, start_link, [Params]},
         temporary, infinity, supervisor, [amqp_connection_sup]}).

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
