-module(just_request_acceptor).

-include("gateway.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("alley_common/include/logging.hrl").

-define(name(UUID), {UUID, request_acceptor}).
-define(pid(UUID), gproc:lookup_local_name(?name(UUID))).

-behaviour(gen_server).

%% API exports.
-export([start_link/3]).
-export([stop/1]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-record(st, {uuid :: binary(),
             name :: string(),
             settings :: just_settings:settings(),
             dedup :: pid(),
             sup :: pid(),
             chan :: pid(),
             queue :: binary(),
             ctag :: binary(),
             processors :: ets:tid()}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}, pid(), pid()) -> {ok, pid()}.
start_link(Gateway, Dedup, ProcessorSup) ->
    gen_server:start_link(?MODULE, [Gateway, Dedup, ProcessorSup], []).

-spec stop(binary()) -> ok.
stop(UUID) ->
    gen_server:call(?pid(UUID), stop, infinity).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([Gateway, Dedup, ProcessorSup]) ->
    #gateway{uuid = UUID, name = Name, settings = Settings} = Gateway,
    ?log_info("Gateway #~s#: initializing request acceptor", [Name]),
    gproc:add_local_name(?name(UUID)),
    Queue = just_app:get_env(request_queue_prefix) ++
            binary_to_list(uuid:unparse_lower(UUID)),
    ?log_info("Gateway #~s#: request acceptor will consume from ~s",
               [Name, Queue]),
    St = setup_chan(#st{uuid = UUID, name = Name, settings = Settings,
                        dedup = Dedup, sup = ProcessorSup,
                        queue = list_to_binary(Queue),
                        processors = ets:new(processors, [])}),
    ?log_info("Gateway #~s#: initialized request acceptor", [Name]),
    {ok, St}.

terminate(_Reason, _St) ->
    ok.

handle_call(stop, _From, St) ->
    ?log_info("Gateway #~s#: stopping request acceptor", [St#st.name]),
    St1 = close_chan(wait_for_processors(unsubscribe(St))),
    ?log_info("Gateway #~s#: stopped request acceptor", [St#st.name]),
    {stop, normal, ok, St1};

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info({#'basic.deliver'{consumer_tag = CTag} = Deliver, Content},
            #st{ctag = CTag} = St) ->
    #'basic.deliver'{delivery_tag = DTag} = Deliver,
    #amqp_msg{payload = Payload, props = Props} = Content,
    #'P_basic'{message_id = MessageId, content_type = CType} = Props,
    case just_dedup:is_dup(St#st.dedup, MessageId) of
        true ->
            ?log_warn("Gateway #~s#: ignoring duplicate request ~s",
                      [St#st.name, MessageId]),
            just_amqp:ack(St#st.chan, DTag),
            {noreply, St};
        false ->
            {noreply, handle_deliver(DTag, MessageId, CType, Payload, St)}
    end;

handle_info({#'basic.deliver'{}, _Content}, St) ->
    {noreply, St};

handle_info({'DOWN', _MRef, process, Pid, _Info}, #st{chan = Pid} = St) ->
    {noreply, setup_chan(wait_for_processors(St#st{chan = undefined,
                                                   ctag = undefined}))};

%% A request processor exited (most likely normally).
handle_info({'DOWN', _MRef, process, Pid, Reason}, St) ->
    {noreply, handle_processor_down(Pid, Reason, St)};

handle_info(amqp_conn_ready, St) ->
    {noreply, setup_chan(St)};

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% handle basic.deliver and processor down
%% -------------------------------------------------------------------------

handle_deliver(DTag, MessageId, ContentType, Payload, St) ->
    case length(ets:match(St#st.processors, {'_', '_', MessageId})) of
        0 ->
            Pid = just_request_processor_sup:start_processor(St#st.sup, St#st.uuid),
            monitor(process, Pid),
            ets:insert(St#st.processors, {Pid, DTag, MessageId}),
            just_request_processor:process(Pid, ContentType, Payload, St#st.settings),
            St;
        1 ->
            % a duplicate of a request is being processed at the moment.
            just_amqp:ack(St#st.chan, DTag),
            St
    end.

handle_processor_down(Pid, Reason, St) ->
    [{Pid, DTag, MessageId}] = ets:lookup(St#st.processors, Pid),
    ets:delete(St#st.processors, Pid),
    just_dedup:insert(St#st.dedup, MessageId),
    case St#st.chan of
        undefined -> ok;
        Chan      -> just_amqp:ack(Chan, DTag)
    end,
    case Reason of
        normal -> ok;
        _      -> ?log_error("Gateway #~s#: failed to process request ~s (~w)",
                              [St#st.name, MessageId, Reason])
    end,
    St.

%% -------------------------------------------------------------------------
%% other private functions
%% -------------------------------------------------------------------------

wait_for_processors(St) ->
    case ets:info(St#st.processors, size) of
        0 ->
            St;
        _ ->
            Chan = St#st.chan,
            receive
                {'DOWN', _MRef, process, Chan, _Info} ->
                    wait_for_processors(St#st{chan = undefined, ctag = undefined});
                {'DOWN', _MRef, process, Pid, Reason} ->
                    wait_for_processors(handle_processor_down(Pid, Reason, St))
            end
    end.

setup_chan(St) ->
    case just_amqp_conn:open_channel(St#st.uuid) of
        {ok, Chan} ->
            monitor(process, Chan),
            ok = just_amqp:declare_queue(Chan, St#st.queue),
			Qos = just_app:get_env(amqp_qos),
			ok = just_amqp:qos(Chan, Qos),
            {ok, CTag} = just_amqp:subscribe(Chan, St#st.queue),
            St#st{chan = Chan, ctag = CTag};
        unavailable ->
            St
    end.

unsubscribe(St) ->
    case St#st.ctag of
        undefined ->
            St;
        CTag ->
            just_amqp:unsubscribe(St#st.chan, CTag),
            St#st{ctag = undefined}
    end.

close_chan(St) ->
    case St#st.chan of
        undefined ->
            ok;
        Chan ->
            amqp_channel:close(Chan),
            St#st{chan = undefined}
    end.
