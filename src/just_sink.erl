-module(just_sink).

-include("gateway.hrl").
-include("persistence.hrl").
-include_lib("alley_common/include/logging.hrl").
-include_lib("alley_dto/include/JustAsn.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(name(UUID, Type), {UUID, sink, Type}).
-define(pid(UUID, Type), gproc:lookup_local_name(?name(UUID, Type))).

-define(NOVALUE_IF_EQ(Value, CompareTo),
        case Value of CompareTo -> asn1_NOVALUE; _ -> Value end).

-behaviour(gen_server).

%% API exports.
-export([start_link/2]).
-export([stop/2, notify/4]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-type type() :: response | incoming | receipt.

-record(st, {uuid :: binary(),
             name :: string(),
             type :: type(),
             queue :: binary(),
             toke :: pid(),
             chan :: pid(),
             unconfirmed :: ets:tid(),
             backlog :: gb_tree()}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}, type()) -> {ok, pid()}.
start_link(Gateway, Type) ->
    gen_server:start_link(?MODULE, [Gateway, Type], []).

-spec stop(binary(), type()) -> ok.
stop(GatewayUUID, Type) ->
    gen_server:call(?pid(GatewayUUID, Type), stop, infinity).

%% @doc Notify the sink that a message or a receipt has been inserted into
%% the tokyo cabinet table.
-spec notify(binary(), type(), just_time:precise_time(), binary()) -> ok.
notify(GatewayUUID, Type, AcceptedAt, UUID) ->
    gen_server:cast(?pid(GatewayUUID, Type), {notify, AcceptedAt, UUID}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([Gateway, Type]) ->
    #gateway{uuid = UUID, name = Name} = Gateway,
    ?log_info("Gateway #~s#: initializing ~s sink", [Name, Type]),
    gproc:add_local_name(?name(UUID, Type)),
    Toke = just_cabinets:table(UUID, Type),
    Backlog = init_state(Toke),
    ?log_info("Gateway #~s#: ~w ~ss in the backlog",
              [Name, backlog_size(Backlog), Type]),
    Key = case Type of
              response -> response_queue;
              incoming -> incoming_queue;
              receipt  -> receipt_queue
          end,
    Queue = list_to_binary(just_app:get_env(Key)),
    ?log_info("Gateway #~s#: ~s sink will publish to ~s",
              [Name, Type, Queue]),
    St = setup_chan(#st{uuid = UUID, type = Type, name = Name,
                        backlog = Backlog, queue = Queue, toke = Toke,
                        unconfirmed = ets:new(unconfirmed, [ordered_set])}),
    ?log_info("Gateway #~s#: initialized ~s sink", [Name, Type]),
    {ok, maybe_publish_backlog(St)}.

terminate(_Reason, _St) ->
    ok.

handle_call(stop, _From, St) ->
    ?log_info("Gateway #~s#: stopping ~s sink", [St#st.name, St#st.type]),
    teardown_chan(St),
    flush_acks(St),
    ?log_info("Gateway #~s#: stopped ~s sink", [St#st.name, St#st.type]),
    {stop, normal, ok, St};

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast({notify, AcceptedAt, UUID}, St) ->
    case publish(AcceptedAt, UUID, St) of
        true ->
            {noreply, St};
        false ->
            {noreply, St#st{backlog = backlog_in(AcceptedAt, UUID, St#st.backlog)}}
    end;

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info(Confirm, St) when is_record(Confirm, 'basic.ack');
                              is_record(Confirm, 'basic.nack') ->
    {noreply, handle_confirm(Confirm, St)};

handle_info({'DOWN', _MRef, process, Pid, _Info}, #st{chan = Pid} = St) ->
    Backlog = lists:foldl(fun({_DTag, AcceptedAt, UUID}, Acc) ->
                              backlog_in(AcceptedAt, UUID, Acc)
                          end, St#st.backlog, ets:tab2list(St#st.unconfirmed)),
    ets:delete_all_objects(St#st.unconfirmed),
    {noreply, maybe_publish_backlog(setup_chan(St#st{backlog = Backlog,
                                                     chan = undefined}))};

handle_info(amqp_conn_ready, St) ->
    {noreply, maybe_publish_backlog(setup_chan(St))};

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% channel setup and teardown
%% -------------------------------------------------------------------------

setup_chan(St) ->
    case just_amqp_conn:open_channel(St#st.uuid) of
        {ok, Chan} ->
            monitor(process, Chan),
            amqp_channel:register_confirm_handler(Chan, self()),
            ok = just_amqp:confirm_select(Chan),
            ok = just_amqp:declare_queue(Chan, St#st.queue),
            St#st{chan = Chan};
        unavailable ->
            St
    end.

teardown_chan(St) ->
    try
        amqp_channel:wait_for_confirms(St#st.chan)
    catch
        _:{noproc, _} -> ok
    after
        amqp_channel:close(St#st.chan)
    end.

%% -------------------------------------------------------------------------
%% init state from a tokyo table
%% -------------------------------------------------------------------------

init_state(Toke) ->
    toke_drv:fold(fun(UUID, Bin, Backlog) ->
                      backlog_in(accepted_at(binary_to_term(Bin)), UUID, Backlog)
                  end, backlog_new(), Toke).

accepted_at(#message{accepted_at = At})  -> At;
accepted_at(#receipt{accepted_at = At})  -> At;
accepted_at(#response{accepted_at = At}) -> At.

%% -------------------------------------------------------------------------
%% handle basic.ack and basic.nack
%% -------------------------------------------------------------------------

flush_acks(St) ->
    receive
        #'basic.ack'{} = Ack ->
            flush_acks(handle_confirm(Ack, St))
    after
        0 ->
            ok
    end.

handle_confirm(#'basic.ack'{delivery_tag = DTag, multiple = Multiple}, St) ->
    case Multiple of
        false -> do_handle_ack(DTag, St);
        true  -> [ do_handle_ack(D, St) || D <- dtags_upto(DTag, St) ]
    end,
    St;

handle_confirm(#'basic.nack'{delivery_tag = DTag, multiple = Multiple}, St) ->
    case Multiple of
        false -> do_handle_nack(DTag, St);
        true  -> lists:foldl(fun(D, S) -> do_handle_nack(D, S) end,
                             St, dtags_upto(DTag, St))
    end.

do_handle_ack(DTag, St) ->
    {DTag, _AcceptedAt, UUID} = fetch_unconfirmed(DTag, St),
    toke_drv:delete(St#st.toke, UUID).

do_handle_nack(DTag, St) ->
    {DTag, AcceptedAt, UUID} = fetch_unconfirmed(DTag, St),
    case publish(AcceptedAt, UUID, St) of
        true  -> St;
        false -> St#st{backlog = backlog_in(AcceptedAt, UUID, St#st.backlog)}
    end.

fetch_unconfirmed(DTag, St) ->
    [Object] = ets:lookup(St#st.unconfirmed, DTag),
    ets:delete(St#st.unconfirmed, DTag),
    Object.

dtags_upto(Max, St) ->
    case ets:first(St#st.unconfirmed) of
        '$end_of_table'      -> [];
        DTag when DTag > Max -> [];
        DTag                 -> dtags_upto(Max, St, [DTag])
    end.

dtags_upto(Max, St, Acc) ->
    case ets:next(St#st.unconfirmed, hd(Acc)) of
        '$end_of_table'      -> Acc;
        DTag when DTag > Max -> Acc;
        DTag                 -> dtags_upto(Max, St, [DTag|Acc])
    end.

%% -------------------------------------------------------------------------
%% backlog functions
%% -------------------------------------------------------------------------

backlog_new() ->
    gb_trees:empty().

backlog_in(At, UUID, B) ->
    case gb_trees:lookup(At, B) of
        none ->
            gb_trees:insert(At, [UUID], B);
        {value, UUIDs} ->
            gb_trees:update(At, [UUID|UUIDs], B)
    end.

backlog_out(B) ->
    case gb_trees:is_empty(B) of
        true ->
            empty;
        false ->
            {At, [UUID|T], B1} = gb_trees:take_smallest(B),
            B2 = case T of
                     [] -> B1;
                     _  -> gb_trees:update(At, T, B)
                 end,
            {At, UUID, B2}
    end.

backlog_size(B) ->
    backlog_size(gb_trees:iterator(B), 0).

backlog_size(Iter, Size) ->
    case gb_trees:next(Iter) of
        {_, UUIDs, Iter1} -> backlog_size(Iter1, Size + length(UUIDs));
        none              -> Size
    end.

%% -------------------------------------------------------------------------
%% publish single/backlog
%% -------------------------------------------------------------------------

publish(_AcceptedAt, _UUID, #st{chan = undefined}) ->
    false;
publish(AcceptedAt, UUID, St) ->
    case toke_drv:get(St#st.toke, UUID) of
        not_found ->
            ?log_error("Gateway #~s#: ~s ~s not found in the hash table",
                       [St#st.name, St#st.type, uuid:unparse_lower(UUID)]),
            true;
        Bin ->
            {Payload, Pbasic} = encode(St#st.type, St#st.uuid, UUID, Bin),
            case just_amqp:publish(St#st.chan, Payload, Pbasic, St#st.queue) of
                ok ->
                    Seqno = amqp_channel:next_publish_seqno(St#st.chan) - 1,
                    ets:insert(St#st.unconfirmed, {Seqno, AcceptedAt, UUID}),
                    true;
                {error, Reason} ->
                    ?log_error("Gateway #~s#: AMQP error when publishing a ~s (~s)",
                               [St#st.name, St#st.type, Reason]),
                    false
            end
    end.

maybe_publish_backlog(#st{chan = undefined} = St) ->
    St;
maybe_publish_backlog(St) ->
    case backlog_out(St#st.backlog) of
        empty ->
            St;
        {AcceptedAt, UUID, Backlog1} ->
            case publish(AcceptedAt, UUID, St) of
                true ->
                    maybe_publish_backlog(St#st{backlog = Backlog1});
                false ->
                    St
            end
    end.

%% -------------------------------------------------------------------------
%% encode (response | incoming | receipt) !REMAINS OF A LEGACY API!
%% -------------------------------------------------------------------------

encode(response, GatewayUUID, SegmentUUID, Bin) ->
    #response{batch = BatchUUID, customer = CustomerUUID,
              dest = Dest, sar_total_segments = Total,
              segment_results = SegmentResults} = binary_to_term(Bin),
    SmStatuses = [ sm_status(Dest, Total, SR) || SR <- SegmentResults ],
    {Seconds, _MilliSeconds} = just_time:precise_time(),
    Timestamp = just_time:unix_time_to_utc_string(Seconds),
    SmsResponse = #'SmsResponse'{id = uuid:unparse_lower(BatchUUID),
                                 gatewayId = uuid:unparse_lower(GatewayUUID),
                                 customerId = uuid:unparse_lower(CustomerUUID),
                                 statuses = SmStatuses,
                                 timestamp = Timestamp},
    {ok, Encoded} = 'JustAsn':encode('SmsResponse', SmsResponse),
    Pbasic = #'P_basic'{content_type = <<"SmsResponse">>, delivery_mode = 2,
                        message_id = uuid:unparse_lower(SegmentUUID)},
    {Encoded, Pbasic};

encode(incoming, GatewayUUID, MessageUUID, Bin) ->
    #message{orig = Orig, dest = Dest, body = Body, data_coding = DC,
             accepted_at = {Seconds, _MilliSeconds}, sar_total_segments = Total,
             sar_segment_seqnum = Seqnum,
             sar_msg_ref_num = RefNum} = binary_to_term(Bin),
    Timestamp = just_time:unix_time_to_utc_string(Seconds),
    IncomingSm = #'IncomingSm'{gatewayId = uuid:unparse_lower(GatewayUUID),
                               source = full_addr(Orig),
                               dest = full_addr(Dest),
                               message = Body, dataCoding = DC,
                               partsRefNum = ?NOVALUE_IF_EQ(RefNum, undefined),
                               partsCount = ?NOVALUE_IF_EQ(Total, 1),
                               partIndex = ?NOVALUE_IF_EQ(Seqnum, undefined),
                               timestamp = Timestamp},
    {ok, Encoded} = 'JustAsn':encode('IncomingSm', IncomingSm),
    Pbasic = #'P_basic'{content_type = <<"IncomingSm">>, delivery_mode = 2,
                        message_id = uuid:unparse_lower(MessageUUID)},
    {Encoded, Pbasic};

encode(receipt, GatewayUUID, ReceiptUUID, Bin) ->
    #receipt{orig = Orig, message_id = MsgID, message_state = MsgState,
             accepted_at = {Seconds, _MilliSeconds}} = binary_to_term(Bin),
    Timestamp = just_time:unix_time_to_utc_string(Seconds),
    DeliveryReceipt = #'DeliveryReceipt'{messageId = MsgID,
                                         messageState = MsgState,
                                         source = full_addr(Orig)},
    ReceiptBatch = #'ReceiptBatch'{gatewayId = uuid:unparse_lower(GatewayUUID),
                                   receipts = [DeliveryReceipt],
                                   timestamp = Timestamp},
    {ok, Encoded} = 'JustAsn':encode('ReceiptBatch', ReceiptBatch),
    Pbasic = #'P_basic'{content_type = <<"ReceiptBatch">>, delivery_mode = 2,
                        message_id = uuid:unparse_lower(ReceiptUUID)},
    {Encoded, Pbasic}.

full_addr(#addr{addr = Addr, ton = Ton, npi = Npi}) ->
    #'FullAddr'{addr = Addr, ton = Ton, npi = Npi}.

sm_status(Dest, Total, SR) ->
    #segment_result{sar_segment_seqnum = Seqnum, orig_msg_id = ID} = SR,
    S = #'SmStatus'{originalId = ID, destAddr = full_addr(Dest),
                    partsTotal = Total,
                    partIndex = ?NOVALUE_IF_EQ(Seqnum, undefined)},
    case SR#segment_result.result of
        {ok, MsgID}   -> S#'SmStatus'{status = success, messageId = MsgID};
        {error, Code} -> S#'SmStatus'{status = failure, errorCode = Code}
    end.
