-module(just_smpp_client).

-include("gateway.hrl").
-include("persistence.hrl").
-include_lib("oserl/include/oserl.hrl").
-include_lib("alley_common/include/logging.hrl").

-behaviour(gen_server).
-behaviour(gen_esme_session).

%% API exports.
-export([start_link/8]).
-export([stop/1, connect/1, bind/1, unbind/1, start_metronome/1,
         submit/4, enqueue_submit/3, to_list/1]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

%% gen_esme_session exports
-export([handle_accept/2, handle_alert_notification/2, handle_outbind/2,
         handle_resp/3, handle_operation/2, handle_enquire_link/2,
         handle_unbind/2, handle_closed/2, handle_timeout/3]).

-define(THROTTLED_PAUSE, 500). % milliseconds before resuming submits.

-define(gv(Key, Params), proplists:get_value(Key, Params)).
-define(gv(Key, Params, Default), proplists:get_value(Key, Params, Default)).

-record(st, {uuid :: binary(),
             name :: string(),
             rps :: pos_integer(),
             settings :: just_settings:settings(),
             conn :: #smpp_connection{},
             sup :: pid(),
             metronome :: pid(),
             log_mgr :: pid(),
             req_tab :: ets:tid(),
             session :: pid(),
             wl = [] :: [{pid(), any()}], % waiting list (blocked metronome workers).
             throttled = false :: false | {true, reference()}}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(binary(), string(), pos_integer(), just_settings:settings(),
                 #smpp_connection{}, pid(), pid(), pid()) -> {ok, pid()}.
start_link(UUID, Name, RPS, Settings, Conn, ClientSup, Metronome, LogMgr) ->
    Args = [UUID, Name, RPS, Settings, Conn, ClientSup, Metronome, LogMgr],
    gen_server:start_link(?MODULE, Args, []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

-spec connect(pid()) -> ok | {error, any()}.
connect(Pid) ->
    gen_server:call(Pid, connect, infinity).

-spec bind(pid()) -> {ok, list()} | {error, any()}.
bind(Pid) ->
    gen_server:call(Pid, bind, infinity).

-spec unbind(pid()) -> {ok, list()} | {error, any()}.
unbind(Pid) ->
    gen_server:call(Pid, unbind, infinity).

-spec start_metronome(pid()) -> ok.
start_metronome(Pid) ->
    gen_server:cast(Pid, start_metronome).

%% @doc Async submit_sm, called by metronome workers.
%% Immediately returns either 'ok' or 'blocked' (if the client is
%% currentrly throttled or the window is full). In the latter case
%% the worker is put into the waiting queue and will be notified when
%% the client becomes unthrottled or a free slot appears.
%% The real (SMSC) response will be returned to From (the process that had initially
%% put the request into a just_submit_queue process - a just_worker).
-spec submit(pid(), list(), {pid(), reference()}, any()) -> ok | blocked.
submit(Pid, Params, From, NotifyMsg) ->
    gen_server:call(Pid, {submit, Params, From, NotifyMsg}, infinity).

%% @doc Enqueues a submit_sm request in the gateway's shared submit queue.
%% From there it will be picked up by one of the submit metronome workers
%% and submitted to a specific client. The real (SMSC) response will be returned
%% to the process that called enqueue_submit as a {Tag, Response} message.
-spec enqueue_submit(binary(), list(), just_time:precise_time()) -> reference().
enqueue_submit(UUID, Params, Expiry) ->
    Tag = make_ref(),
    From = {self(), Tag},
    just_submit_queue:in(just_submit_queue:pid(UUID), {Params, From}, Expiry),
    Tag.

-spec to_list(#smpp_connection{}) -> string().
to_list(Conn) ->
    #smpp_connection{id = ID, type = Type, addr = Addr, port = Port,
                     system_id = SystemId} = Conn,
    lists:concat([ID, "_", Type, "_", Addr, "_", Port, "_", SystemId]).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([UUID, Name, RPS, Settings, Conn, ClientSup, Metronome, LogMgr]) ->
    case just_settings:get(log_smpp_pdus, Settings) of
        true ->
            pmm_smpp_logger_h:add_to_manager(LogMgr),
            Dir = filename:join([just_app:get_env(file_log_dir), "smpp",
                                 uuid:unparse_lower(UUID)]),
            file:make_dir(Dir),
            LogParams = [{base_dir, Dir},
                         {base_file_name, to_list(Conn) ++ ".log"},
                         {max_size, just_app:get_env(file_log_size)}],
            pmm_smpp_logger_h:activate(LogMgr, LogParams);
        false ->
            ok
    end,
    {ok, #st{uuid = UUID, name = Name, rps = RPS, settings = Settings, conn = Conn,
             sup = ClientSup, metronome = Metronome, log_mgr = LogMgr,
             req_tab = ets:new(req_tab, [])}}.

terminate(_Reason, St) ->
    [ gen_server:reply(From, {error, closed}) ||
        {_Ref, From} <- ets:tab2list(St#st.req_tab) ].

handle_call(stop, _From, St) ->
    just_submit_metronome:stop(St#st.metronome),
    {stop, normal, ok, close_session(St)};

handle_call(connect, _From, St) ->
    S = fun(Key) -> just_settings:get(Key, St#st.settings) end,
    Timers = ?TIMERS(?SESSION_INIT_TIME,
                     S(smpp_enquire_link_time),
                     S(smpp_inactivity_time),
                     S(smpp_response_time)),
    #smpp_connection{addr = Addr, port = Port} = St#st.conn,
    Args = [{log, St#st.log_mgr}, {timers, Timers}, {ip, S(ip)},
            {addr, Addr}, {port, Port}, {esme, self()}],
    case just_smpp_client_sup:start_esme_session(St#st.sup, Args) of
        {ok, Pid} ->
            monitor(process, Pid),
            {reply, ok, St#st{session = Pid}};
        {error, {Reason, _}} ->
            {stop, normal, {error, Reason}, St}
    end;

handle_call(bind, From, St) ->
    #smpp_connection{type = Type, system_id = SystemId, password = Password,
                     system_type = SystemType, addr_ton = Ton,
                     addr_npi = Npi, addr_range = Range} = St#st.conn,
    F = case Type of
            transmitter -> bind_transmitter;
            receiver    -> bind_receiver;
            transceiver -> bind_transceiver
        end,
    Args = [{system_id, SystemId},
            {password, Password},
            {system_type, SystemType},
            {interface_version, just_settings:get(smpp_version, St#st.settings)},
            {addr_ton, Ton},
            {addr_npi, Npi},
            {address_range, Range}],
    Ref = gen_esme_session:F(St#st.session, Args),
    ets:insert(St#st.req_tab, {Ref, From}),
    % reply will be delivered from handle_cast({handle_resp, ..).
    {noreply, St};

handle_call(unbind, From, St) ->
    Ref = gen_esme_session:unbind(St#st.session),
    ets:insert(St#st.req_tab, {Ref, From}),
    % reply will be delivered from handle_cast({handle_resp, ..).
    {noreply, St};

handle_call({submit, Params, From, NotifyMsg}, {Pid, _Tag}, St) ->
    case is_throttled(St) orelse unacked_request_count(St) >= window_size(St) of
        false ->
            Ref = gen_esme_session:submit_sm(St#st.session, Params),
            ets:insert(St#st.req_tab, {Ref, From}),
            just_throughput:incr(St#st.uuid, St#st.name, sms_out, 1),
            {reply, ok, St};
        true ->
            {reply, blocked, St#st{wl = [{Pid, NotifyMsg}|St#st.wl]}}
    end;

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast(start_metronome, St) ->
    #smpp_connection{type = Type} = St#st.conn,
    case Type of
        receiver -> ok;
        _        -> just_submit_metronome:start(St#st.metronome, self())
    end,
    {noreply, St};

handle_cast({handle_resp, Resp, Ref}, St) ->
    [{Ref, From}] = ets:lookup(St#st.req_tab, Ref),
    ets:delete(St#st.req_tab, Ref),
    case Resp of
        {ok, {_CmdId, _Status, _SeqNum, Body}} ->
            gen_server:reply(From, {ok, Body});
        {error, {command_status, Status}} ->
            gen_server:reply(From, {error, Status})
    end,
    RTHROTTLED = (Resp =:= {error, {command_status, ?ESME_RTHROTTLED}}),
    St1 = case RTHROTTLED orelse is_throttled(St) of
              true ->
                  handle_throttling(RTHROTTLED, St);
              false ->
                  [ Pid ! NotifyMsg || {Pid, NotifyMsg} <- St#st.wl ],
                  St#st{wl = []}
          end,
    {noreply, St1};

%% TODO LATER: spawn a process to handle each deliver_sm.
handle_cast({handle_deliver_sm, SeqNum, Body}, St) ->
    just_throughput:incr(St#st.uuid, St#st.name, sms_in, 1),
    IsReceipt =
        ?gv(esm_class, Body) band ?ESM_CLASS_TYPE_MC_DELIVERY_RECEIPT =:=
                ?ESM_CLASS_TYPE_MC_DELIVERY_RECEIPT,
    case IsReceipt of
        true ->
            handle_receipt(Body, St);
        false ->
            handle_message(Body, St)
    end,
    % TODO LATER: do not always return ok.
    gen_esme_session:reply(St#st.session, {SeqNum, {ok, []}}),
    {noreply, St};

handle_cast(handle_unbind, St) ->
    ?log_warn("Gateway #~s#: connection ~s unbound by peer",
              [St#st.name, to_list(St#st.conn)]),
    {stop, normal, close_session(St)};

handle_cast({handle_closed, closed}, St) ->
    ?log_error("Gateway #~s#: connection ~s socket closed while bound",
                [St#st.name, to_list(St#st.conn)]),
    {stop, normal, St};

handle_cast({handle_closed, Reason}, St) ->
    % some other socket error. shouldn't happen.
    ?log_error("Gateway #~s#: connection ~s socket error while bound (~w)",
                [St#st.name, to_list(St#st.conn), Reason]),
    {stop, {socket_error, Reason}, St};

handle_cast({handle_timeout, SeqNum, Ref}, St) ->
    ?log_warn("Gateway #~s#: timeout seq_num: ~p ref: ~p",
              [St#st.name, SeqNum, Ref]),
    {noreply, St};

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info({'DOWN', _MRef, process, Pid, normal}, #st{session = Pid} = St) ->
    % the socket was closed while the session was unbound.
    {stop, normal, St#st{session = undefined}};

handle_info({'DOWN', _MRef, process, Pid, Info}, #st{session = Pid} = St) ->
    % let this go to the crash log.
    {stop, {session_down, Info}, St#st{session = undefined}};

handle_info({timeout, TRef, unthrottle}, #st{throttled = {true, TRef}} = St) ->
    {noreply, handle_unthrottle_timeout(St)};

%% a cancelled timer.
handle_info({timeout, _TRef, _}, St) ->
    {noreply, St};

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% gen_esme_session callback functions
%% -------------------------------------------------------------------------

%% only needed for listening clients that support outbind
handle_accept(_Esme, _Addr) ->
    {error, forbidden}.

%% we do not use data_sm and thus never set delivery_pending flag
handle_alert_notification(_Esme, _Pdu) ->
    ok.

%% we do not provide outbind support
handle_outbind(_Esme, _Pdu) ->
    ok.

%% Delivers an async repsonse to the request with Ref.
handle_resp(Esme, Resp, Ref) ->
    gen_server:cast(Esme, {handle_resp, Resp, Ref}).

%% Handle SMSC-issued deliver_sm operation.
handle_operation(Esme, {deliver_sm, {_CmdId, _Status, SeqNum, Body}}) ->
    gen_server:cast(Esme, {handle_deliver_sm, SeqNum, Body}),
    noreply;

%% Return ?ESME_RINVCMDID error for any other operation.
handle_operation(_Esme, {_Cmd, _Pdu}) ->
    {error, ?ESME_RINVCMDID}.

%% Forwards enquire_link operations (from the peer MC) to the callback module.
handle_enquire_link(_Esme, _Pdu) ->
    ok.

%% Handle SMSC-issued unbind.
handle_unbind(Esme, _Pdu) ->
    gen_server:cast(Esme, handle_unbind).

%% Notify Client of the Reason before stopping the session.
handle_closed(Esme, Reason) ->
    gen_server:cast(Esme, {handle_closed, Reason}).

handle_timeout(Esme, SeqNum, Ref) ->
    gen_server:cast(Esme, {handle_timeout, SeqNum, Ref}).

%% -------------------------------------------------------------------------
%% handle throttling
%% -------------------------------------------------------------------------

is_throttled(St) ->
    St#st.throttled =/= false.

handle_throttling(RTHROTTLED, St) ->
    case {RTHROTTLED, is_throttled(St)} of
        {true, false} ->
            ?log_warn("Gateway #~s#: connection ~s got throttled",
                      [St#st.name, to_list(St#st.conn)]),
            just_smpp_clients:throttled(St#st.uuid, St#st.conn),
            just_submit_metronome:pause(St#st.metronome),
            start_throttled_timer(St#st{wl = []});
        {false, true} ->
            ?log_info("Gateway #~s#: connection ~s unthrottled",
                      [St#st.name, to_list(St#st.conn)]),
            just_smpp_clients:unthrottled(St#st.uuid, St#st.conn),
            just_submit_metronome:resume(St#st.metronome),
            cancel_throttled_timer(St);
        {true, true} ->
            restart_throttled_timer(St)
    end.

handle_unthrottle_timeout(St) ->
    ?log_info("Gateway #~s#: connection ~s unthrottled",
              [St#st.name, to_list(St#st.conn)]),
    just_smpp_clients:unthrottled(St#st.uuid, St#st.conn),
    just_submit_metronome:resume(St#st.metronome),
    St#st{throttled = false}.

start_throttled_timer(St) ->
    TRef = erlang:start_timer(?THROTTLED_PAUSE, self(), unthrottle),
    St#st{throttled = {true, TRef}}.

cancel_throttled_timer(St) ->
    {true, TRef} = St#st.throttled,
    erlang:cancel_timer(TRef),
    St#st{throttled = false}.

restart_throttled_timer(St) ->
    start_throttled_timer(cancel_throttled_timer(St)).

%% -------------------------------------------------------------------------
%% handle a receipt in deliver_sm
%% -------------------------------------------------------------------------

handle_receipt(Body, St) ->
    AcceptedAt = just_time:precise_time(),
    {ID, State} = receipt_data(Body),
    R = #receipt{orig = #addr{addr = ?gv(source_addr, Body),
                              ton = ?gv(source_addr_ton, Body),
                              npi = ?gv(source_addr_npi, Body)},
                 message_id = ID,
                 message_state = State,
                 accepted_at = AcceptedAt},
    UUID = uuid:generate(),
    toke_drv:insert(just_cabinets:table(St#st.uuid, receipt), UUID,
                    term_to_binary(R)),
    just_sink:notify(St#st.uuid, receipt, AcceptedAt, UUID).

receipt_data(Body) ->
    case receipt_data_from_tlv(Body) of
        false -> receipt_data_from_text(Body);
        Data  -> Data
    end.

receipt_data_from_tlv(Body) ->
    ID = ?gv(receipted_message_id, Body),
    State = ?gv(message_state, Body),
    case ID =/= undefined andalso State =/= undefined of
        true  -> {list_to_binary(ID), state_to_atom(State)};
        false -> false
    end.

receipt_data_from_text(Body) ->
    Text = ?gv(short_message, Body),
    Opts = [caseless, {capture, all_but_first, binary}],
    {match, [ID]} = re:run(Text, "id:([[:xdigit:]]+)", Opts),
    {match, [State]} = re:run(Text, "stat:(\\w+)", Opts),
    {ID, state_to_atom(State)}.

state_to_atom(S) when S =:= 1; S =:= <<"ENROUTE">> -> enroute;
state_to_atom(S) when S =:= 2; S =:= <<"DELIVRD">> -> delivered;
state_to_atom(S) when S =:= 3; S =:= <<"EXPIRED">> -> expired;
state_to_atom(S) when S =:= 4; S =:= <<"DELETED">> -> deleted;
state_to_atom(S) when S =:= 5; S =:= <<"UNDELIV">> -> undeliverable;
state_to_atom(S) when S =:= 6; S =:= <<"ACCEPTD">> -> accepted;
state_to_atom(S) when S =:= 7; S =:= <<"UNKNOWN">> -> unknown;
state_to_atom(S) when S =:= 8; S =:= <<"REJECTD">> -> rejected;
state_to_atom(_)                                   -> unrecognized.

%% -------------------------------------------------------------------------
%% handle a non-receipt in deliver_sm
%% -------------------------------------------------------------------------

handle_message(Body, St) ->
    AcceptedAt = just_time:precise_time(),
    P = fun(Param) -> ?gv(Param, Body) end,
    DC = P(data_coding),
    {{Total, Seqnum, RefNum}, Text} = extract_sar_info(Body),
    M = #message{orig = #addr{addr = P(source_addr),
                              ton = P(source_addr_ton),
                              npi = P(source_addr_npi)},
                 dest = #addr{addr = P(destination_addr),
                              ton = P(dest_addr_ton),
                              npi = P(dest_addr_npi)},
                 body = decode_text(DC, list_to_binary(Text)),
                 data_coding = DC,
                 sar_total_segments = Total,
                 sar_segment_seqnum = Seqnum,
                 sar_msg_ref_num = RefNum,
                 accepted_at = AcceptedAt},
    UUID = uuid:generate(),
    toke_drv:insert(just_cabinets:table(St#st.uuid, incoming), UUID,
                    term_to_binary(M)),
    just_sink:notify(St#st.uuid, incoming, AcceptedAt, UUID).

extract_sar_info(Body) ->
    case smpp_sm:udhi(Body) of
        true ->
            % TODO: maybe support 16 bit concat IE.
            {UDH, Text} = smpp_sm:chop_udh(?gv(short_message, Body)),
            try smpp_sm:ie(?IEI_CONCAT, UDH) of
                [?IEI_CONCAT, ?IEDL_CONCAT, RefNum, Total, Seqnum] ->
                    {{Total, Seqnum, RefNum}, Text}
            catch
                error:function_clause ->
                    {{1, undefined, undefined}, Text}
            end;
        false ->
            {{?gv(sar_total_segments, Body, 1),
              ?gv(sar_segment_seqnum, Body),
              ?gv(sar_msg_ref_num, Body)},
             ?gv(short_message, Body)}
    end.

decode_text(DC, Text) when DC =:= 0; DC =:= 16; DC =:= 240 ->
    % gsm 0338.
    gsm0338:to_utf8(Text);
decode_text(3, Text) ->
    % latin1.
    {ok, Decoded} = iconverl:conv("utf-8//IGNORE", "latin1", Text),
    Decoded;
decode_text(DC, Text) when DC =:= 8; DC =:= 24 ->
    % ucs-2be.
    {ok, Decoded} = iconverl:conv("utf-8//IGNORE", "ucs-2be", Text),
    Decoded;
decode_text(_, Text) ->
    % 8-bit binary or ascii or anything else.
    Text.

%% -------------------------------------------------------------------------
%% other private functions
%% -------------------------------------------------------------------------

window_size(St) ->
    just_settings:get(smpp_window_size, St#st.settings).

unacked_request_count(St) ->
    ets:info(St#st.req_tab, size).

close_session(St) ->
    try gen_esme_session:stop(St#st.session) of
        ok -> ok
    catch
        _:{noproc, _} ->
            ok
    end,
    St#st{session = undefined}.
