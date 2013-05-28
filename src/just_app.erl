-module(just_app).

-behaviour(application).

%% API exports
-export([get_env/1]).
-export([set_debug_level/0]).

%% application callback exports
-export([start/2, prep_stop/1, stop/1, config_change/3]).

-define(APP, just_mini).

-include_lib("queue_fabric/include/queue_fabric.hrl").

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec get_env(atom()) -> any().
get_env(Key) ->
    case application:get_env(?APP, Key) of
        {ok, Val} -> Val;
        undefined -> default_env(Key)
    end.

-spec set_debug_level() -> ok.
set_debug_level() ->
	lager:set_loglevel(lager_console_backend, debug).

%% -------------------------------------------------------------------------
%% application callback functions
%% -------------------------------------------------------------------------

start(normal, _StartArgs) ->
    ok = just_mib:ensure_mnesia_tables(infinity),
    ok = load_mibs(),
    just_mib:send_coldstart_notification(),
    just_sup:start_link().

%% This function is called when ?APP application is about to be stopped,
%% before shutting down the processes of the application.
prep_stop(St) ->
    just_gateways:stop(),
    St.

%% Perform necessary cleaning up *after* ?APP application has stopped.
stop(_St) ->
    unload_mibs().

config_change(_Changed, _New, _Removed) ->
    ok.

%% -------------------------------------------------------------------------
%% private functions
%% -------------------------------------------------------------------------

default_env(amqp_username)        -> <<"guest">>;
default_env(amqp_password)        -> <<"guest">>;
default_env(amqp_vhost)           -> <<"/">>;
default_env(amqp_host)            -> "localhost";
default_env(amqp_port)            -> 5672;
default_env(amqp_qos)			  -> 1000;
default_env(request_queue_prefix) -> ?JUST_GTW_Q_PREFIX;
default_env(response_queue)       -> ?RESPONSE_SMS_Q;
default_env(message_queue)        -> ?INCOMING_SMS_Q;
default_env(receipt_queue)        -> ?RECEIPT_SMS_Q;
default_env(control_queue) 		  -> ?JUST_CONTROL_Q;
default_env(file_log_dir)         -> "log";
default_env(file_log_size)        -> 5000000;
default_env(replies_deadline)     -> 120.

load_mibs() ->
    ok = otp_mib:load(snmp_master_agent),
    ok = os_mon_mib:load(snmp_master_agent),
    ok = snmpa:load_mibs(snmp_master_agent, [just_mib()]).

unload_mibs() ->
    snmpa:unload_mibs(snmp_master_agent, [just_mib()]),
    os_mon_mib:unload(snmp_master_agent),
    otp_mib:unload(snmp_master_agent).

just_mib() ->
    filename:join(code:priv_dir(?APP), "mibs/JUST-MIB").
