-module(just_request_processor).

-include_lib("alley_dto/include/JustAsn.hrl").
-include("persistence.hrl").

-behaviour(gen_server).

%% API exports.
-export([start_link/1]).
-export([process/4]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-define(gv(Key, Params), proplists:get_value(Key, Params)).
-define(gv(Key, Params, Default), proplists:get_value(Key, Params, Default)).
-define(gb(Key, Params), proplists:get_bool(Key, Params)).
-define(gs(Name, Settings), just_settings:get(Name, Settings)).

-record(st, {uuid :: binary(), toke :: pid()}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(binary()) -> {ok, pid()}.
start_link(UUID) ->
    gen_server:start_link(?MODULE, [UUID], []).

-spec process(pid(), binary(), binary(), just_settings:settings()) -> ok.
process(Pid, ContentType, Payload, Settings) ->
    gen_server:cast(Pid, {process, ContentType, Payload, Settings}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([UUID]) ->
    {ok, #st{uuid = UUID, toke = just_cabinets:table(UUID, request)}}.

terminate(_Reason, _St) ->
    ok.

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast({process, ContentType, Payload, Settings}, St) ->
    AcceptedAt = just_time:precise_time(),
    lists:foreach(
        fun(R) ->
            U = uuid:generate(),
            toke_drv:insert(St#st.toke, U, term_to_binary(R)),
            just_scheduler:notify(St#st.uuid, R#request.customer, U,
                                  length(R#request.payload), AcceptedAt)
        end,
        transform_request(ContentType, Payload, AcceptedAt, Settings)
    ),
    {stop, normal, St};

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% (ContentType, Payload) -> [#request{}] transformation
%% -------------------------------------------------------------------------

transform_request(<<"SmsRequest", _>>, Payload, AcceptedAt, Settings) ->
    transform_asn(Payload, AcceptedAt, Settings).

%% -------------------------------------------------------------------------
%% #'SmsRequest' -> [#request{}] transformation
%% -------------------------------------------------------------------------

transform_asn(Payload, AcceptedAt, Settings) ->
    {ok, SmsRequest} = 'JustAsn':decode('SmsRequest', Payload),
    #'SmsRequest'{id = Id, customerId = CustomerId,
                  sourceAddr = SourceAddr} = SmsRequest,
    BatchUUID = uuid:parse(list_to_binary(Id)),
    CustomerUUID = uuid:parse(list_to_binary(CustomerId)),
    #'FullAddr'{addr = Addr, ton = Ton, npi = Npi} = SourceAddr,
    Params = params_asn_to_proplist(SmsRequest#'SmsRequest'.params),
    PortAddressing = port_addressing(Params),
    Message = SmsRequest#'SmsRequest'.message,
    Encoding = SmsRequest#'SmsRequest'.encoding,
    Type = SmsRequest#'SmsRequest'.type,
    {Message2, DC} = process_payload(
        list_to_binary(Message), Encoding, Type, Params, Settings, PortAddressing),
    Type2 = request_type(Type, Message2),
    VP = ?gv(validity_period, Params, ""),
    ExpiresAt = expiration_time(AcceptedAt, VP, Settings),
    RD = case ?gb(registered_delivery, Params) of true -> 1; false -> 0 end,
    Common = #request{batch = BatchUUID,
                      customer = CustomerUUID,
                      type = Type2,
                      attempt_once = ?gb(no_retry, Params),
                      payload = Message2,
                      data_coding = DC,
                      orig = #addr{addr = Addr, ton = Ton, npi = Npi},
                      validity_period = VP,
                      service_type = ?gv(service_type, Params),
                      protocol_id = ?gv(protocol_id, Params),
                      priority_flag = ?gv(priority_flag, Params),
                      registered_delivery = RD,
                      port_addressing = PortAddressing,
                      attempt_at = AcceptedAt,
                      expires_at = ExpiresAt,
                      accepted_at = AcceptedAt},
    complete_asn(Type2, Params, Common, SmsRequest#'SmsRequest'.messageIds,
             element(2, SmsRequest#'SmsRequest'.destAddrs)).

complete_asn(Type, Params, Common, Ids, Addrs) ->
    complete_asn(Type, Params, Common, Ids, Addrs, []).

complete_asn(_Type, _Params, _Common, [], [], Acc) ->
    Acc;

complete_asn(short, Params, Common, [Id|Ids], [Dest|Dests], Acc) ->
    #'FullAddr'{addr = Addr, ton = Ton, npi = Npi} = Dest,
    C = Common#request{info = #short_info{orig_msg_id = list_to_binary(Id)},
                       dest = #addr{addr = Addr, ton = Ton, npi = Npi},
                       todo_segments = [1]},
    complete_asn(short, Params, Common, Ids, Dests, [C|Acc]);

complete_asn(long, Params, Common, [Id|Ids], [Dest|Dests], Acc) ->
    #'FullAddr'{addr = Addr, ton = Ton, npi = Npi} = Dest,
    {RefNum, _} = random:uniform_s(255, now()),
    Info = #long_info{orig_msg_ids = re:split(Id, ":", [trim, {return, binary}]),
                      sar_msg_ref_num = RefNum},
    C = Common#request{info = Info,
                       dest = #addr{addr = Addr, ton = Ton, npi = Npi},
                       todo_segments = lists:seq(1, length(Common#request.payload))},
    complete_asn(long, Params, Common, Ids, Dests, [C|Acc]);

complete_asn(segment, Params, Common, [Id|Ids], [Dest|Dests], Acc) ->
    #'FullAddrAndRefNum'{fullAddr = #'FullAddr'{addr = Addr, ton = Ton, npi = Npi},
                         refNum = RefNum} = Dest,
    Info = #segment_info{orig_msg_id = list_to_binary(Id),
                         sar_msg_ref_num = RefNum,
                         sar_total_segments = ?gv(sar_total_segments, Params),
                         sar_segment_seqnum = ?gv(sar_segment_seqnum, Params)},
    C = Common#request{info = Info,
                       dest = #addr{addr = Addr, ton = Ton, npi = Npi},
                       todo_segments = [1]},
    complete_asn(segment, Params, Common, Ids, Dests, [C|Acc]).

params_asn_to_proplist(Params) ->
    [{list_to_atom(N), V} || #'Param'{name = N, value = {_, V}} <- Params].

%% -------------------------------------------------------------------------
%% generic body encoding and splitting
%% -------------------------------------------------------------------------

process_payload(Message, Encoding, Type, Params, Settings, PortAddressing) ->
    {Encoding2, DC, Bitness} = encoding_dc_bitness(Encoding, Params, Settings),
    Message2 = encode_msg(Message, Encoding2),
    Payload =
        case Type of
            regular ->
                split_msg(Message2, Bitness, PortAddressing);
            part ->
                [Message2]
         end,
    {Payload, DC}.

encoding_dc_bitness(Encoding, Params, Settings) ->
    {E, DC, B} =
        case Encoding of
            {text, default} ->
                {?gs(default_encoding, Settings),
                 ?gs(default_data_coding, Settings),
                 ?gs(default_bitness, Settings)};
            {text, gsm0338} ->
                {gsm0338, 0, 7};
            {text, ascii} ->
                {ascii, 1, 7};
            {text, latin1} ->
                {latin1, 3, 8};
            {text, ucs2} ->
                {ucs2, 8, 16};
            {other, Other} ->
                {other, Other, 8}
        end,
    {E, ?gv(data_coding, Params, DC), B}.

encode_msg(Msg, gsm0338) ->
    gsm0338:from_utf8(Msg);
encode_msg(Msg, ascii) ->
    Msg;
encode_msg(Msg, latin1) ->
    {ok, Encoded} = iconverl:conv("latin1//IGNORE", "utf-8", Msg),
    Encoded;
encode_msg(Msg, ucs2) ->
    {ok, Encoded} = iconverl:conv("ucs-2be//IGNORE", "utf-8", Msg),
    Encoded;
encode_msg(Msg, other) ->
    Msg.

max_msg_len(Bitness, undefined) ->
    case Bitness of
        7  -> {160, 153};
        8  -> {140, 134};
        16 -> {140, 134}
    end;
max_msg_len(Bitness, _) ->
    case Bitness of
        7  -> {152, 146};
        8  -> {133, 128};
        16 -> {132, 128}
    end.

split_msg(Msg, Bitness, PortAddressing) ->
    {MaxWhole, MaxPart} = max_msg_len(Bitness, PortAddressing),
    case size(Msg) > MaxWhole of
        true  -> just_helpers:split_binary(Msg, MaxPart);
        false -> [Msg]
    end.

%% -------------------------------------------------------------------------
%% generic calculate expiration time
%% -------------------------------------------------------------------------

expiration_time(AcceptedAt, VP, Settings) ->
    {S, MS} = AcceptedAt,
    HardLimit = {S + ?gs(max_validity_period, Settings) * 60 * 60, MS},
    IsAtime = cl_string:is_atime(VP) andalso VP =/= "",
    IsRtime = cl_string:is_rtime(VP) andalso VP =/= "",
    if
        IsAtime ->
            erlang:min(HardLimit, parse_atime(VP));
        IsRtime ->
            erlang:min(HardLimit, {S + parse_rtime(VP), MS});
        true ->
            HardLimit
    end.

parse_atime([Y1,Y2,Mon1,Mon2,D1,D2,H1,H2,Min1,Min2,S1,S2,T,N1,N2,P]) ->
    DT = {{2000 + list_to_integer([Y1,Y2]),
           list_to_integer([Mon1,Mon2]),
           list_to_integer([D1,D2])},
          {list_to_integer([H1,H2]),
           list_to_integer([Min1,Min2]),
           list_to_integer([S1,S2])}},
    Seconds = just_time:datetime_to_unix_time(DT),
    Sign = case P of $+ -> 1; $- -> -1 end,
    Diff = list_to_integer([N1,N2]) * 15 * 60 * Sign,
    {Seconds - Diff, list_to_integer([T]) * 100}.

parse_rtime([Y1,Y2,Mon1,Mon2,D1,D2,H1,H2,Min1,Min2,S1,S2,$0,$0,$0,$R]) ->
    Y = list_to_integer([Y1,Y2]),
    Mon = list_to_integer([Mon1,Mon2]),
    D = list_to_integer([D1,D2]),
    H = list_to_integer([H1,H2]),
    Min = list_to_integer([Min1,Min2]),
    S = list_to_integer([S1,S2]),
    (((Y * 365 + Mon * 31 + D) * 24 + H) * 60 + Min) * 60 + S.

%% -------------------------------------------------------------------------
%% other generic private functions
%% -------------------------------------------------------------------------

port_addressing(Params) ->
    DestPort = ?gv(destination_port, Params),
    OrigPort = ?gv(source_port, Params),
    case DestPort =/= undefined andalso OrigPort =/= undefined of
        true  -> #port_addressing{dest = DestPort, orig = OrigPort};
        false -> undefined
    end.

request_type(Type, Message) ->
    case {Type, length(Message)} of
        {regular, 1} -> short;
        {regular, _} -> long;
        {part, 1}    -> segment
    end.
