-module(just_worker).

-include("persistence.hrl").
-include_lib("oserl/include/oserl.hrl").
-include_lib("alley_common/include/logging.hrl").

-behaviour(gen_server).

%% API exports.
-export([start_link/3]).
-export([work/2, kill/2]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-define(ifundef(Val, Key, Settings),
        case Val of
            undefined -> just_settings:get(Key, Settings);
            _         -> Val
        end).

-define(gv(Key, Params), proplists:get_value(Key, Params)).
-define(gv(Key, Params, Default), proplists:get_value(Key, Params, Default)).
-define(gs(Key, Settings), just_settings:get(Key, Settings)).

%% Keep in sync with k_sms_response_handler.erl
-define(ERROR_TIMEOUT,  16#000000400).
-define(ERROR_CLOSED,   16#000000401).
-define(ERROR_EXPIRED,  16#000000402).
-define(ERROR_CUSTOMER, 16#000000403).
-define(ERROR_BLOCKED,  16#000000404).

-record(st, {uuid :: binary(),
             name :: string(),
             settings :: just_settings:settings(),
             request :: pid(),    % tokyo table.
             response :: pid()}). % tokyo table.

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(binary(), string(), just_settings:settings()) -> {ok, pid()}.
start_link(UUID, Name, Settings) ->
    gen_server:start_link(?MODULE, [UUID, Name, Settings], []).

-spec work(pid(), binary()) -> ok.
work(Pid, ReqUUID) ->
    gen_server:cast(Pid, {work, ReqUUID}).

-spec kill(pid(), binary()) -> ok.
kill(Pid, ReqUUID) ->
    gen_server:cast(Pid, {kill, ReqUUID}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([UUID, Name, Settings]) ->
    {ok, #st{uuid = UUID, name = Name, settings = Settings,
             request = just_cabinets:table(UUID, request),
             response = just_cabinets:table(UUID, response)}}.

terminate(_Reason, _St) ->
    ok.

handle_call(Req, _From, St) ->
    {stop, {unexpected_call, Req}, St}.

handle_cast({work, ReqUUID}, St) ->
    case toke_drv:get(St#st.request, ReqUUID) of
        not_found ->
            ?log_error("Gateway #~s#: request ~s not found in the hash table",
                       [St#st.name, uuid:unparse_lower(ReqUUID)]);
        Bin ->
            Req = binary_to_term(Bin),
            case just_time:precise_time() >= Req#request.expires_at of
                true ->
                    do_kill(expired, Req, ReqUUID, St);
                false ->
                    case just_request_blocker:is_blocked(Req#request.batch) of
                        true ->
                            do_kill(blocked, Req, ReqUUID, St);
                        false ->
                            do_work(Req, ReqUUID, St)
                    end
            end
    end,
    {stop, normal, St};

handle_cast({kill, ReqUUID}, St) ->
    case toke_drv:get(St#st.request, ReqUUID) of
        not_found ->
            ?log_error("Gateway #~s#: request ~s not found in the hash table",
                       [St#st.name, uuid:unparse_lower(ReqUUID)]);
        Bin ->
            do_kill(customer, binary_to_term(Bin), ReqUUID, St)
    end,
    {stop, normal, St};

handle_cast(Req, St) ->
    {stop, {unexpected_cast, Req}, St}.

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% handle work, kill (expired/inactive customer)
%% -------------------------------------------------------------------------

do_work(Req, ReqUUID, St) ->
    Time = just_time:precise_time(),
    RT = ?gs(smpp_response_time, St#st.settings),
    Expires = add_ms(Time, RT),
    SegNums = Req#request.todo_segments,
    PDUs = list_nths(SegNums, smpp_pdus(Req, St#st.settings)),
    Refs = [ just_smpp_client:enqueue_submit(St#st.uuid, PDU, Expires)
            || PDU <- PDUs ],
    Replies = collect_replies(length(Refs), RT),
    {Attempts, Done, NotDone} = split_replies(Refs, Replies, Req, St#st.settings),
    % match segment indexes with replies (done only).
    SegNumsReplies = [ {?gv(Ref, lists:zip(Refs, SegNums)), Reply}
                      || {Ref, Reply} <- Done ],
    SegmentResults = [ segment_result(SNR, Req) || SNR <- SegNumsReplies ],
    DoneSegNums = [ SegNum || {SegNum, _Reply} <- SegNumsReplies ],
    Req1 = Req#request{attempts = Attempts,
                       done_segments = Req#request.done_segments ++ SegmentResults,
                       todo_segments = Req#request.todo_segments -- DoneSegNums},
    case NotDone of
        [] -> respond(Req1, ReqUUID, St);
        _  -> reschedule(Req1, ReqUUID, St)
    end.

do_kill(Reason, Req, ReqUUID, St) ->
    Code = case Reason of
               expired  -> ?ERROR_EXPIRED;
               customer -> ?ERROR_CUSTOMER;
               blocked  -> ?ERROR_BLOCKED
           end,
    SegNums = Req#request.todo_segments,
    SegNumsReplies = [ {SegNum, {error, Code}} || SegNum <- SegNums ],
    SegmentResults = [ segment_result(SNR, Req) || SNR <- SegNumsReplies ],
    respond(Req#request{done_segments = Req#request.done_segments ++ SegmentResults},
            ReqUUID, St).

respond(Req, UUID, St) ->
    Resp = #response{batch = Req#request.batch,
                     customer = Req#request.customer,
                     dest = Req#request.dest,
                     sar_total_segments = length(Req#request.payload),
                     segment_results = Req#request.done_segments,
                     accepted_at = Req#request.accepted_at},
    toke_drv:insert(St#st.response, UUID, term_to_binary(Resp)),
    just_sink:notify(St#st.uuid, response, Req#request.accepted_at, UUID),
    toke_drv:delete(St#st.request, UUID).

reschedule(Req, UUID, St) ->
    % determine the retry delay, reinsert the updated request into
    % the hash table, notify the scheduler.
    Delay = case Req#request.attempts > ?gs(fast_retry_times, St#st.settings) + 1 of
                true  -> ?gs(slow_retry_delay, St#st.settings);
                false -> ?gs(fast_retry_delay, St#st.settings)
            end,
    AttemptAt = erlang:min(add_ms(just_time:precise_time(), Delay),
                           Req#request.expires_at),
    toke_drv:insert(St#st.request, UUID,
                    term_to_binary(Req#request{attempt_at = AttemptAt})),
    just_scheduler:notify(St#st.uuid, Req#request.customer, UUID,
                          length(Req#request.todo_segments), AttemptAt).

segment_result({SegNum, Reply}, Req) ->
    Result = case Reply of
                 {ok, MessageId} ->
                     {ok, list_to_binary(MessageId)};
                 {error, Code} ->
                     {error, Code};
                 {error, Code, _Cost} ->
                     {error, Code}
             end,
    #segment_result{sar_segment_seqnum = segment_seqnum(SegNum, Req),
                    orig_msg_id = segment_msg_id(SegNum, Req),
                    result = Result}.

segment_seqnum(SegNum, Req) ->
    case Req#request.type of
        short   -> undefined;
        long    -> SegNum;
        segment -> (Req#request.info)#segment_info.sar_segment_seqnum
    end.

segment_msg_id(SegNum, Req) ->
    case Req#request.type of
        short ->
            (Req#request.info)#short_info.orig_msg_id;
        long ->
            lists:nth(SegNum, (Req#request.info)#long_info.orig_msg_ids);
        segment ->
            (Req#request.info)#segment_info.orig_msg_id
    end.

%% -------------------------------------------------------------------------
%% colection and sorting of replies
%% -------------------------------------------------------------------------

%% split all replies into Done and NotDone lists.
split_replies(Refs, Replies, Req, Settings) ->
    {Done, Unsure} = sort_replies(Refs, Replies, Settings),
    MaxAttempts =
        case Req#request.attempt_once of
            true ->
                0;
            false ->
                ?gs(slow_retry_times, Settings) +
                ?gs(fast_retry_times, Settings) + 1
        end,
    TotalCost = lists:sum([ Cost || {_, {_, _, Cost}} <- Unsure ]),
    Attempts = Req#request.attempts + erlang:min(1, TotalCost),
    case Attempts < MaxAttempts of
        true ->
            {Attempts, Done, Unsure};
        false ->
            {Attempts, Done ++ Unsure, []}
    end.

sort_replies(Refs, Replies, Settings) ->
    sort_replies(Refs, Replies, Settings, {[], []}).

sort_replies([], _Replies, _Settings, Acc) ->
    Acc;
sort_replies([Ref|Refs], Replies, Settings, {Done, Unsure}) ->
    case sort_reply(?gv(Ref, Replies), Settings) of
        {done, Reply} ->
            sort_replies(Refs, Replies, Settings, {[{Ref, Reply}|Done], Unsure});
        {unsure, Reply} ->
            sort_replies(Refs, Replies, Settings, {Done, [{Ref, Reply}|Unsure]})
    end.

sort_reply(Reply, Settings) ->
    PR = preprocess_reply(Reply, Settings),
    case PR of
        {ok, _MessageId}         -> {done, PR};
        {error, _Code, infinity} -> {done, PR};
        {error, _Code, _Cost}    -> {unsure, PR}
    end.

preprocess_reply(Reply, Settings) ->
    case Reply of
        {ok, Params} ->
            {ok, ?gv(message_id, Params)};
        undefined ->
            {error, ?ERROR_TIMEOUT, 0}; % timed out.
        {error, closed} ->
            {error, ?ERROR_CLOSED, 0};
        {error, timeout} ->
            {error, ?ERROR_TIMEOUT, 0};
        {error, Code} ->
            {error, Code, error_cost(Code, Settings)}
    end.

error_cost(?ESME_RTHROTTLED, _Settings) ->
    0;
error_cost(Code, Settings) ->
    Terminal = lists:member(Code, ?gs(terminal_errors, Settings)),
    Discard = lists:member(Code, ?gs(discarded_errors, Settings)),
    if
        Terminal -> infinity;
        Discard  -> 0;
        true     -> 1
    end.

%% collect replies from the clients, stop after Time or after completion.
collect_replies(N, Time) ->
    TRef = erlang:start_timer(Time, self(), stop),
    collect_replies(N, TRef, []).

collect_replies(0, TRef, Acc) ->
    erlang:cancel_timer(TRef),
    Acc;
collect_replies(N, TRef, Acc) ->
    receive
        {timeout, TRef, stop} ->
            Acc;
        {Ref, Reply} ->
            collect_replies(N - 1, TRef, [{Ref, Reply}|Acc])
    end.

%% -------------------------------------------------------------------------
%% SMPP PDU preparation
%% -------------------------------------------------------------------------

is_segmented(Req) ->
    Req#request.type =/= short.

has_port_addressing(Req) ->
    Req#request.port_addressing =/= undefined.

use_sar_tlv(Settings) ->
    just_settings:get(sar_method, Settings) =:= sar_tlv.

smpp_pdus(Req, Settings) ->
    Orig = Req#request.orig,
    Dest = Req#request.dest,
    HasUDH = (not use_sar_tlv(Settings)) andalso
             (is_segmented(Req) orelse has_port_addressing(Req)),
    Common = [{service_type, Req#request.service_type},
              {source_addr_ton, Orig#addr.ton},
              {source_addr_npi, Orig#addr.npi},
              {source_addr, Orig#addr.addr},
              {dest_addr_ton, Dest#addr.ton},
              {dest_addr_npi, Dest#addr.npi},
              {destination_addr, Dest#addr.addr},
              {esm_class, case HasUDH of true -> ?ESM_CLASS_GSM_UDHI; false -> 0 end},
              {protocol_id,
               ?ifundef(Req#request.protocol_id, smpp_protocol_id, Settings)},
              {priority_flag,
               ?ifundef(Req#request.priority_flag, smpp_priority_flag, Settings)},
              {validity_period, Req#request.validity_period},
              {registered_delivery, Req#request.registered_delivery},
              {data_coding, Req#request.data_coding}],
    case Req#request.type of
        short ->
            [smpp_pdu(short, Common, Req, Settings)];
        long ->
            smpp_pdus(long, Common, Req, Settings);
        segment ->
            [smpp_pdu(segment, Common, Req, Settings)]
    end.

smpp_pdu(short, Common, Req, Settings) ->
    Payload = binary_to_list(hd(Req#request.payload)),
    case has_port_addressing(Req) of
        true ->
            #port_addressing{dest = DestPort, orig = OrigPort} =
                Req#request.port_addressing,
            case use_sar_tlv(Settings) of
                true ->
                    [{dest_port, DestPort},
                     {source_port, OrigPort},
                     {short_message, Payload}
                     |Common];
                false ->
                    IE = pmm_udh:port_addressing_ie(DestPort, OrigPort),
                    [{short_message, [length(IE)|IE ++ Payload]}|Common]
            end;
        false ->
            [{short_message, Payload}|Common]
    end;

smpp_pdu(segment, Common, Req, Settings) ->
    Payload = binary_to_list(hd(Req#request.payload)),
    #segment_info{sar_msg_ref_num = RefNum,
                  sar_total_segments = TotalSegments,
                  sar_segment_seqnum = Seqnum} = Req#request.info,
    segment_pdu(RefNum, TotalSegments, Seqnum, Req, Payload, Common, Settings).

smpp_pdus(long, Common, Req, Settings) ->
    Payloads = [ binary_to_list(P) || P <- Req#request.payload ],
    TotalSegments = length(Payloads),
    #long_info{sar_msg_ref_num = RefNum} = Req#request.info,
    lists:map(fun({Seqnum, Payload}) ->
                  segment_pdu(RefNum, TotalSegments, Seqnum,
                              Req, Payload, Common, Settings)
              end,
              lists:zip(lists:seq(1, TotalSegments), Payloads)).

segment_pdu(RefNum, TotalSegments, Seqnum, Req, Payload, Common, Settings) ->
    case has_port_addressing(Req) of
        true ->
            #port_addressing{dest = DestPort, orig = OrigPort} =
                Req#request.port_addressing,
            case use_sar_tlv(Settings) of
                true ->
                    [{dest_port, DestPort},
                     {source_port, OrigPort},
                     {sar_msg_ref_num, RefNum},
                     {sar_total_segments, TotalSegments},
                     {sar_segment_seqnum, Seqnum},
                     {short_message, Payload}
                     |Common];
                false ->
                    IEs = pmm_udh:concat_8_ie(RefNum, TotalSegments, Seqnum) ++
                          pmm_udh:port_addressing_ie(DestPort, OrigPort),
                    [{short_message, [length(IEs)|IEs ++ Payload]}|Common]
            end;
        false ->
            case use_sar_tlv(Settings) of
                true ->
                    [{sar_msg_ref_num, RefNum},
                     {sar_total_segments, TotalSegments},
                     {sar_segment_seqnum, Seqnum},
                     {short_message, Payload}
                     |Common];
                false ->
                    IE = pmm_udh:concat_8_ie(RefNum, TotalSegments, Seqnum),
                    [{short_message, [length(IE)|IE ++ Payload]}|Common]
             end
    end.

%% -------------------------------------------------------------------------
%% other private functions
%% -------------------------------------------------------------------------

add_ms({S, MS}, N) ->
    {S + (MS + N) div 1000, (MS + N) rem 1000}.

list_nths(Ns, List) ->
    [ lists:nth(N, List) || N <- Ns ].
