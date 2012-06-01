-module(just_publisher).

-include("persistence.hrl").
-include("JustAsn.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(NOVALUE_IF_EQ(Value, CompareTo),
        case Value of CompareTo -> asn1_NOVALUE; _ -> Value end).

-behaviour(gen_server).

%% API exports.
-export([start_link/4]).
-export([publish/2]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-type type() :: message | receipt | response.

-record(st, {uuid :: binary(), name :: string(), toke :: pid(), chan :: pid(),
             type :: type()}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(binary(), string(), type(), pid()) ->
    {ok, pid()}.
start_link(UUID, Name, Type, Chan) ->
    gen_server:start_link(?MODULE, [UUID, Name, Type, Chan], []).

-spec publish(pid(), list()) -> ok.
publish(Pid, What) ->
    gen_server:cast(Pid, {publish, What}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([UUID, Name, Type, Chan]) ->
    {ok, #st{uuid = UUID, name = Name, type = Type, chan = Chan,
             toke = just_cabinets:table(UUID, Type)}}.

terminate(_Reason, _St) ->
    ok.

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast({publish, What}, St) ->
    Key = case St#st.type of
              response -> response_queue;
              message  -> message_queue;
              receipt  -> receipt_queue
          end,
    Queue = list_to_binary(just_app:get_env(Key)),
    [ ok = just_amqp:publish(St#st.chan, Payload, Pbasic, Queue)
      || {Payload, Pbasic} <- prepare(St, What) ],
    ok = just_amqp:tx_commit(St#st.chan),
    cleanup_cabinet(St, What),
    {stop, normal, St};

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% message encoding
%% -------------------------------------------------------------------------

prepare(St, What) ->
    prepare(St, What, []).

prepare(_St, [], Acc) ->
    Acc;

prepare(#st{type = response} = St,  [{_, [], _}|Tail], Acc) ->
    prepare(St, Tail, Acc);
prepare(#st{type = response} = St,
                [{BatchUUID, [UUID|SegmentsTail], AcceptedAt}|Tail], Acc) ->
    #st{uuid = GatewayUUID, name = Name, toke = Toke} = St,
    Next = [{BatchUUID, SegmentsTail, AcceptedAt}|Tail],
    case toke_drv:get(Toke, UUID) of
        not_found ->
            lager:error("Gateway #~s#: response ~s not found in the hash table",
                        [Name, uuid:unparse_lower(UUID)]),
            prepare(St, Next, Acc);
        Bin ->
            prepare(St, Next, [prepare_response(GatewayUUID, UUID, Bin)|Acc])
    end;

%% messages or receipts.
prepare(St, [UUID|Tail], Acc) ->
    #st{uuid = GatewayUUID, name = Name, type = Type, toke = Toke} = St,
    case toke_drv:get(Toke, UUID) of
        not_found ->
            lager:error("Gateway #~s#: ~s ~s not found in the hash table",
                        [Name, Type, uuid:unparse_lower(UUID)]),
            prepare(St, Tail, Acc);
        Bin ->
            prepare(St, Tail, [prepare_incoming(Type, GatewayUUID, UUID, Bin)|Acc])
    end.

full_addr(#addr{addr = Addr, ton = Ton, npi = Npi}) ->
    #'FullAddr'{addr = Addr, ton = Ton, npi = Npi}.

prepare_response(GatewayUUID, SegmentUUID, Bin) ->
    #response{batch = BatchUUID, customer = CustomerUUID, dest = Dest,
              sar_total_segments = Total, segment_results = SegmentResults,
              accepted_at = {Seconds, _}} = binary_to_term(Bin),
    SmStatuses = [ sm_status(Dest, Total, SR) || SR <- SegmentResults ],
    Timestamp = just_time:unix_time_to_utc_string(Seconds),
    SmsResponse = #'SmsResponse'{id = uuid:unparse_lower(BatchUUID),
                                 gatewayId = uuid:unparse_lower(GatewayUUID),
                                 customerId = uuid:unparse_lower(CustomerUUID),
                                 statuses = SmStatuses,
                                 timestamp = Timestamp},
    {ok, Encoded} = 'JustAsn':encode('SmsResponse', SmsResponse),
    Pbasic = #'P_basic'{content_type = <<"SmsResponse">>, delivery_mode = 2,
                        message_id = uuid:unparse_lower(SegmentUUID)},
    {list_to_binary(Encoded), Pbasic}.

sm_status(Dest, Total, SR) ->
    #segment_result{sar_segment_seqnum = Seqnum, orig_msg_id = ID} = SR,
    S = #'SmStatus'{originalId = ID, destAddr = full_addr(Dest),
                    partsTotal = Total,
                    partIndex = ?NOVALUE_IF_EQ(Seqnum, undefined)},
    case SR#segment_result.result of
        {ok, MsgID}   -> S#'SmStatus'{status = success, messageId = MsgID};
        {error, Code} -> S#'SmStatus'{status = failure, errorCode = Code}
    end.

prepare_incoming(message, GatewayUUID, MessageUUID, Bin) ->
    #message{orig = Orig, dest = Dest, body = Body, data_coding = DC,
             accepted_at = {Seconds, _}, sar_total_segments = Total,
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
    {list_to_binary(Encoded), Pbasic};

prepare_incoming(receipt, GatewayUUID, ReceiptUUID, Bin) ->
    #receipt{orig = Orig, message_id = MsgID, message_state = MsgState,
             accepted_at = {Seconds, MilliSeconds}} = binary_to_term(Bin),
    % legacy format - timestamp is in microseconds here because of
    % some old middleware implementation issues with ordering.
    Timestamp = (Seconds * 1000 + MilliSeconds) * 1000,
    DeliveryReceipt = #'DeliveryReceipt'{messageId = MsgID,
                                         messageState = MsgState,
                                         source = full_addr(Orig)},
    ReceiptBatch = #'ReceiptBatch'{gatewayId = uuid:unparse_lower(GatewayUUID),
                                   receipts = [DeliveryReceipt],
                                   timestamp = Timestamp},
    {ok, Encoded} = 'JustAsn':encode('ReceiptBatch', ReceiptBatch),
    Pbasic = #'P_basic'{content_type = <<"ReceiptBatch">>, delivery_mode = 2,
                        message_id = uuid:unparse_lower(ReceiptUUID)},
    {list_to_binary(Encoded), Pbasic}.

%% -------------------------------------------------------------------------
%% other private functions
%% -------------------------------------------------------------------------

cleanup_cabinet(_St, []) ->
    ok;
cleanup_cabinet(#st{type = response, toke = Toke} = St,
                [{_Batch, Keys, _AcceptedAt}|Tail]) ->
    [ toke_drv:delete(Toke, Key) || Key <- Keys ],
    cleanup_cabinet(St, Tail);
cleanup_cabinet(St, [Key|Tail]) ->
    toke_drv:delete(St#st.toke, Key),
    cleanup_cabinet(St, Tail).
