-module(just_amqp).

-include_lib("amqp_client/include/amqp_client.hrl").

-export([publish/4, ack/2, subscribe/2, unsubscribe/2]).
-export([declare_queue/2]).
-export([confirm_select/1]).

%% -------------------------------------------------------------------------
%% basic functions
%% -------------------------------------------------------------------------

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

%% -------------------------------------------------------------------------
%% confirm functions
%% -------------------------------------------------------------------------

-spec confirm_select(pid()) -> ok.
confirm_select(Channel) ->
    Method = #'confirm.select'{},
    #'confirm.select_ok'{} = amqp_channel:call(Channel, Method),
    ok.
