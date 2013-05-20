 -module(just_amqp_control).

-behaviour(gen_server).

-include_lib("amqp_client/include/amqp_client.hrl").
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
-define(CONTROL_KEY, <<"pmm.just.control">>).

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
    lager:info("amqp control: initializing"),
    schedule_connect(0),
    {ok, #st{}}.

terminate(Reason, St) ->
	just_amqp:connection_close(St#st.conn),
    lager:info("amqp control: terminated (~p)", [Reason]).

handle_call(Call, _From, St) ->
	{stop, {unexpected_call, Call}, St}.

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

%% time to try to connect to the broker.
handle_info({timeout, _Ref, connect}, St) ->
    case just_amqp:connection_start() of
        {ok, Conn} ->
            link(Conn),
            lager:info("amqp control: amqp connection up"),
            try
                {ok, Chan} = just_amqp:channel_open(Conn),
                just_amqp:declare_queue(Chan, ?CONTROL_KEY, false, true, true),
                just_amqp:subscribe(Chan, ?CONTROL_KEY),
                {noreply, St#st{conn = Conn, chan = Chan}}
            catch
                _:_Reason ->
                    % channel and/or connection died in the process.
                    % EXIT will be received.
                    {noreply, St#st{conn = Conn}}
            end;
        {error, Reason} ->
            lager:warning("amqp control: couldn't connect to the broker (~p)", [Reason]),
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
    lager:warning("amqp control: amqp connection down (~p)", [Reason]),
    schedule_connect(?RECONNECT_INTERVAL),
    {noreply, St#st{conn = undefined, chan = undefined}}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% Private functions
%% -------------------------------------------------------------------------

handle_request(<<"ThroughputRequest">>, _Payload) ->
    lager:info("amqp control: got throughput request"),
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
    {list_to_binary(Resp), <<"ThroughputResponse">>};

handle_request(Other, _Payload) ->
    lager:error("amqp control: got unsupported request type (~s)", [Other]),
    gen_nack().

schedule_connect(Time) ->
    erlang:start_timer(Time, self(), connect).

gen_nack() ->
    {ok, Nack} = 'JustAsn':encode('GenNack', #'GenNack'{}),
    {list_to_binary(Nack), <<"GenNack">>}.
