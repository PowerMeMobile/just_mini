-module(just_amqp).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([connection_start/0, connection_close/1]).
-export([channel_open/1]).
-export([qos/2, publish/4, ack/2, subscribe/2, unsubscribe/2]).
-export([declare_queue/5, declare_queue/2]).
-export([confirm_select/1]).

%% -------------------------------------------------------------------------
%% Connection methods
%% -------------------------------------------------------------------------

-spec connection_start() -> {'ok', pid()} | {'error', any()}.
connection_start() ->
    Params = #amqp_params_network{virtual_host = just_app:get_env(amqp_vhost),
                                  username = just_app:get_env(amqp_username),
                                  password = just_app:get_env(amqp_password),
                                  host = just_app:get_env(amqp_host),
                                  port = just_app:get_env(amqp_port)},
	amqp_connection:start(Params).

-spec connection_close(pid()) -> 'ok'.
connection_close(Conn) ->
    catch(amqp_connection:close(Conn)),
    ok.

%% -------------------------------------------------------------------------
%% Channel methods
%% -------------------------------------------------------------------------

-spec channel_open(pid()) -> {'ok', pid()} | {'error', any()}.
channel_open(Conn) ->
    amqp_connection:open_channel(Conn).

%% -------------------------------------------------------------------------
%% basic functions
%% -------------------------------------------------------------------------

-spec qos(pid(), non_neg_integer()) -> 'ok' | {'error', any()}.
qos(Chan, PrefetchCount) ->
    Method = #'basic.qos'{prefetch_count = PrefetchCount},
    try amqp_channel:call(Chan, Method) of
        #'basic.qos_ok'{} -> ok;
        Other             -> {error, Other}
    catch
        _:Reason -> {error, Reason}
    end.

-spec publish(pid(), binary(), #'P_basic'{}, binary()) ->
    ok | {error, noproc | closing}.
publish(Channel, Payload, Properties, RoutingKey) ->
    Method = #'basic.publish'{routing_key = RoutingKey},
    Content = #amqp_msg{props = Properties, payload = Payload},
    try amqp_channel:call(Channel, Method, Content) of
        ok      -> ok;
        closing -> {error, closing}
    catch
        _:{noproc, _} -> {error, noproc}
    end.

-spec ack(pid(), integer()) -> ok | {error, noproc | closing}.
ack(Channel, DeliveryTag) ->
    Method = #'basic.ack'{delivery_tag = DeliveryTag, multiple = false},
    try amqp_channel:call(Channel, Method) of
        ok      -> ok;
        closing -> {error, closing}
    catch
        _:{noproc, _} -> {error, noproc}
    end.

-spec subscribe(pid(), binary()) -> {ok, binary()} | error.
subscribe(Channel, Queue) ->
    Method = #'basic.consume'{queue = Queue, no_ack = false},
    amqp_channel:subscribe(Channel, Method, self()),
    receive
        #'basic.consume_ok'{consumer_tag = CTag} -> {ok, CTag}
    after
        2000 -> error
    end.

-spec unsubscribe(pid(), binary()) -> ok | error.
unsubscribe(Channel, CTag) ->
    Method = #'basic.cancel'{consumer_tag = CTag},
    amqp_channel:call(Channel, Method),
    receive
        #'basic.cancel_ok'{consumer_tag = CTag} -> ok
    after
        2000 -> error
    end.

%% -------------------------------------------------------------------------
%% queue functions
%% -------------------------------------------------------------------------

-spec declare_queue(pid(), binary()) -> ok.
declare_queue(Channel, Queue) ->
    Method = #'queue.declare'{queue = Queue, durable = true},
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Method),
    ok.

-spec declare_queue(pid(), binary(), boolean(), boolean(), boolean()) -> ok.
declare_queue(Channel, Queue, Durable, Exclusive, Autodelete) ->
    Method = #'queue.declare'{
		queue = Queue,
		durable = Durable,
		exclusive = Exclusive,
		auto_delete = Autodelete},
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Method),
    ok.

%% -------------------------------------------------------------------------
%% confirm functions
%% -------------------------------------------------------------------------

-spec confirm_select(pid()) -> ok.
confirm_select(Channel) ->
    Method = #'confirm.select'{},
    #'confirm.select_ok'{} = amqp_channel:call(Channel, Method),
    ok.
