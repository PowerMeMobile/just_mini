-module(just_amqp).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([declare_queue/2]).
-export([tx_select/1, tx_commit/1]).
-export([publish/4, ack/2]).
-export([subscribe/2, unsubscribe/2]).

%% -------------------------------------------------------------------------
%% queue functions
%% -------------------------------------------------------------------------

-spec declare_queue(pid(), binary()) -> ok.
declare_queue(Channel, Queue) ->
    Method = #'queue.declare'{queue = Queue, durable = true},
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Method),
    ok.

%% -------------------------------------------------------------------------
%% tx functions
%% -------------------------------------------------------------------------

-spec tx_select(pid()) -> ok.
tx_select(Channel) ->
    Method = #'tx.select'{},
    #'tx.select_ok'{} = amqp_channel:call(Channel, Method),
    ok.

-spec tx_commit(pid()) -> ok.
tx_commit(Channel) ->
    Method = #'tx.commit'{},
    #'tx.commit_ok'{} = amqp_channel:call(Channel, Method),
    ok.

%% -------------------------------------------------------------------------
%% basic functions
%% -------------------------------------------------------------------------

-spec publish(pid(), binary(), #'P_basic'{}, binary()) -> ok | {error, any()}.
publish(Channel, Payload, Properties, RoutingKey) ->
    Method = #'basic.publish'{routing_key = RoutingKey},
    Content = #amqp_msg{props = Properties, payload = Payload},
    try amqp_channel:call(Channel, Method, Content) of
        ok      -> ok;
        blocked -> {error, blocked};
        closing -> {error, closing}
    catch
        _:Reason -> {error, Reason}
    end.

-spec ack(pid(), integer()) -> ok | {error, any()}.
ack(Channel, DeliveryTag) ->
    Method = #'basic.ack'{delivery_tag = DeliveryTag, multiple = false},
    try amqp_channel:call(Channel, Method) of
        ok -> ok
    catch
        _:Reason -> {error, Reason}
    end.

%% -------------------------------------------------------------------------
%% subscribe and unsubscribe
%% -------------------------------------------------------------------------

-spec subscribe(pid(), binary()) -> {ok, binary()} | error.
subscribe(Channel, Queue) ->
    Method = #'basic.consume'{queue = Queue, no_ack = false},
    amqp_channel:subscribe(Channel, Method, self()),
    receive
        #'basic.consume_ok'{consumer_tag = ConsumerTag} ->
            {ok, ConsumerTag}
    after
        1000 ->
            error
    end.

-spec unsubscribe(pid(), binary()) -> ok | error.
unsubscribe(Channel, ConsumerTag) ->
    Method = #'basic.cancel'{consumer_tag = ConsumerTag},
    amqp_channel:call(Channel, Method),
    receive
        #'basic.cancel_ok'{consumer_tag = ConsumerTag} ->
            ok
    after
        1000 ->
            error
    end.
