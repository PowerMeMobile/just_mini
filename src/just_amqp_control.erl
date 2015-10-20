 -module(just_amqp_control).

-behaviour(gen_server).

-include("gateway.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("alley_common/include/logging.hrl").
-include_lib("alley_dto/include/common_dto.hrl").
-include_lib("alley_dto/include/JustAsn.hrl").

%% API exports
-export([start_link/0]).

%% gen_server exports
-export([init/1,
         terminate/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3]).

-define(RECONNECT_INTERVAL, 5000). % 5 seconds.

-record(st, {conn :: pid(), chan :: pid()}).

%% -------------------------------------------------------------------------
%% API functions
%% -------------------------------------------------------------------------

-spec start_link/0 :: () -> {'ok', pid()} | 'ignore' | {'error', any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    ?log_info("amqp control: initializing", []),
    schedule_connect(0),
    {ok, #st{}}.

terminate(Reason, St) ->
	just_amqp:connection_close(St#st.conn),
    ?log_info("amqp control: terminated (~p)", [Reason]).

handle_call(Call, _From, St) ->
	{stop, {unexpected_call, Call}, St}.

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

%% time to try to connect to the broker.
handle_info({timeout, _Ref, connect}, St) ->
    case just_amqp:connection_start() of
        {ok, Conn} ->
            link(Conn),
            ?log_info("amqp control: amqp connection up", []),
            try
                {ok, Chan} = just_amqp:channel_open(Conn),
                ControlQueue = list_to_binary(just_app:get_env(control_queue)),
                just_amqp:declare_queue(Chan, ControlQueue, false, true, true),
                just_amqp:subscribe(Chan, ControlQueue),
                {noreply, St#st{conn = Conn, chan = Chan}}
            catch
                _:_Reason ->
                    % channel and/or connection died in the process.
                    % EXIT will be received.
                    {noreply, St#st{conn = Conn}}
            end;
        {error, Reason} ->
            ?log_warn("amqp control: couldn't connect to the broker (~p)", [Reason]),
            schedule_connect(?RECONNECT_INTERVAL),
            {noreply, St}
    end;

handle_info({#'basic.deliver'{} = Deliver, Content}, St) ->
    #amqp_msg{payload = Payload, props = Props} = Content,
    #'P_basic'{
        content_type = ContentType,
        message_id   = MsgId,
        reply_to     = ReplyTo
    } = Props,
    {RespPayload, RespContentType} = handle_request(ContentType, Payload),
    RespProps = #'P_basic'{
        content_type   = RespContentType,
        correlation_id = MsgId,
        message_id     = uuid:unparse(uuid:generate())
    },
    just_amqp:publish(St#st.chan, RespPayload, RespProps, ReplyTo),
    just_amqp:ack(St#st.chan, Deliver#'basic.deliver'.delivery_tag),
    {noreply, St};

handle_info({'EXIT', Pid, Reason}, #st{conn = Pid} = St) ->
    ?log_warn("amqp control: amqp connection down (~p)", [Reason]),
    schedule_connect(?RECONNECT_INTERVAL),
    {noreply, St#st{conn = undefined, chan = undefined}}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% Private functions
%% -------------------------------------------------------------------------

handle_request(<<"ThroughputRequest">>, _Payload) ->
    ?log_info("amqp control: got throughput request", []),
    Slices = lists:map(
        fun({PeriodStart, Counters}) ->
                #'Slice'{
                    periodStart = PeriodStart,
                    counters =
                        lists:map(
                            fun({Gtw, Conn, Type, Count}) ->
                                    #'Counter'{
                                        gatewayId = uuid:unparse(Gtw),
                                        gatewayName = Conn,
                                        type =
                                            case Type of
                                                sms_out -> smsOut;
                                                sms_in  -> smsIn
                                            end,
                                        count = Count
                                    }
                            end, Counters
                        )
                }
        end, just_throughput:slices()
    ),
    AsnResp = #'ThroughputResponse'{slices = Slices},
    {ok, Resp} = 'JustAsn':encode('ThroughputResponse', AsnResp),
    {Resp, <<"ThroughputResponse">>};

handle_request(<<"BlockReqV1">>, ReqBin) ->
    {ok, Req} = adto:decode(#block_req_v1{}, ReqBin),
    ?log_info("amqp control: got ~p", [Req]),
    ReqId = Req#block_req_v1.req_id,
    BatchUuid = uuid:parse(Req#block_req_v1.sms_req_id),
    Result = just_request_blocker:block(BatchUuid),
    Resp = #block_resp_v1{
        req_id = ReqId,
        result = Result
    },
    {ok, RespBin} = adto:encode(Resp),
    {RespBin, <<"BlockRespV1">>};

handle_request(<<"UnblockReqV1">>, ReqBin) ->
    {ok, Req} = adto:decode(#unblock_req_v1{}, ReqBin),
    ?log_info("amqp control: got ~p", [Req]),
    ReqId = Req#unblock_req_v1.req_id,
    BatchUuid = uuid:parse(Req#unblock_req_v1.sms_req_id),
    Result = just_request_blocker:unblock(BatchUuid),
    Resp = #unblock_resp_v1{
        req_id = ReqId,
        result = Result
    },
    {ok, RespBin} = adto:encode(Resp),
    {RespBin, <<"UnblockRespV1">>};

handle_request(<<"GatewayStatesReqV1">>, ReqBin) ->
    {ok, Req} = adto:decode(#gateway_states_req_v1{}, ReqBin),
    ?log_info("amqp control: got ~p", [Req]),
    ReqId = Req#gateway_states_req_v1.req_id,
    Result =
        case just_gateways:get_gateway_states() of
            {ok, States} ->
                [gateway_state_to_v1(GS) || GS <- States];
            {error, Reason} ->
                {error, Reason}
        end,
    Resp = #gateway_states_resp_v1{
        req_id = ReqId,
        result = Result
    },
    {ok, RespBin} = adto:encode(Resp),
    {RespBin, <<"GatewayStatesRespV1">>};

handle_request(<<"GatewayStateReqV1">>, ReqBin) ->
    {ok, Req} = adto:decode(#gateway_state_req_v1{}, ReqBin),
    ?log_info("amqp control: got ~p", [Req]),
    ReqId = Req#gateway_state_req_v1.req_id,
    GUuid = uuid:parse(Req#gateway_state_req_v1.gateway_id),
    Result =
        case just_gateways:get_gateway_state(GUuid) of
            {ok, State} ->
                gateway_state_to_v1(State);
            {error, Reason} ->
                {error, Reason}
        end,
    Resp = #gateway_state_resp_v1{
        req_id = ReqId,
        result = Result
    },
    {ok, RespBin} = adto:encode(Resp),
    {RespBin, <<"GatewayStateRespV1">>};

handle_request(<<"StartGatewayReqV1">>, ReqBin) ->
    {ok, Req} = adto:decode(#start_gateway_req_v1{}, ReqBin),
    ?log_info("amqp control: got ~p", [Req]),
    ReqId = Req#start_gateway_req_v1.req_id,
    GUuid = uuid:parse(Req#start_gateway_req_v1.gateway_id),
    Result = just_gateways:start_gateway(GUuid),
    Resp = #start_gateway_resp_v1{
        req_id = ReqId,
        result = Result
    },
    {ok, RespBin} = adto:encode(Resp),
    {RespBin, <<"StartGatewayRespV1">>};

handle_request(<<"StopGatewayReqV1">>, ReqBin) ->
    {ok, Req} = adto:decode(#stop_gateway_req_v1{}, ReqBin),
    ?log_info("amqp control: got ~p", [Req]),
    ReqId = Req#stop_gateway_req_v1.req_id,
    GUuid = uuid:parse(Req#stop_gateway_req_v1.gateway_id),
    Result = just_gateways:stop_gateway(GUuid),
    Resp = #stop_gateway_resp_v1{
        req_id = ReqId,
        result = Result
    },
    {ok, RespBin} = adto:encode(Resp),
    {RespBin, <<"StopGatewayRespV1">>};

handle_request(Other, _Payload) ->
    ?log_error("amqp control: got unsupported request type (~s)", [Other]),
    gen_nack().

schedule_connect(Time) ->
    erlang:start_timer(Time, self(), connect).

gen_nack() ->
    {ok, Nack} = 'JustAsn':encode('GenNack', #'GenNack'{}),
    {Nack, <<"GenNack">>}.

gateway_state_to_v1(GS) ->
    #gateway_state{
        uuid = Uuid,
        name = Name,
        host = Host,
        state = State,
        connections = CSs
    } = GS,
    #gateway_state_v1{
        id = uuid:unparse(Uuid),
        name = list_to_binary(Name),
        host = list_to_binary(Host),
        state = State,
        connections = [connection_state_to_v1(CS) || CS <- CSs]
    }.

connection_state_to_v1(CS) ->
    #connection_state{
        id = Id,
        type = Type,
        addr = Addr,
        port = Port,
        system_id = SystemId,
        password = Password,
        system_type = SystemType,
        state = State
    } = CS,
    #connection_state_v1{
        id = Id,
        host = list_to_binary(Addr),
        port = Port,
        bind_type = Type,
        system_id = list_to_binary(SystemId),
        password = list_to_binary(Password),
        system_type = list_to_binary(SystemType),
        state = State
    }.
