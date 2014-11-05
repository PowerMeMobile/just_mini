-module(just_smpp_clients).

-include("gateway.hrl").
-include_lib("alley_common/include/logging.hrl").

-define(name(UUID), {UUID, smpp_clients}).
-define(pid(UUID), gproc:lookup_local_name(?name(UUID))).

-behaviour(gen_server).

%% API exports.
-export([start_link/2]).
-export([stop/1, throttled/2, unthrottled/2]).

%% gen_server exports.
-export([init/1, terminate/2, handle_cast/2, handle_call/3, handle_info/2,
         code_change/3]).

%% client cluster state
-record(st, {uuid :: binary(),
             name :: string(),
             rps :: pos_integer(),
             settings :: just_settings:settings(),
             sup :: pid(),
             all :: [#smpp_connection{}],
             on = [] :: [#smpp_connection{}],
             off = [] :: [#smpp_connection{}],
             throttled = [] :: [#smpp_connection{}],
             clients :: ets:tid()}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}, pid()) -> {ok, pid()}.
start_link(Gateway, ClientSupSup) ->
    gen_server:start_link(?MODULE, [Gateway, ClientSupSup], []).

-spec stop(binary()) -> ok.
stop(UUID) ->
    gen_server:call(?pid(UUID), stop, infinity).

-spec throttled(binary(), #smpp_connection{}) -> ok.
throttled(UUID, Conn) ->
    gen_server:cast(?pid(UUID), {throttled, Conn}).

-spec unthrottled(binary(), #smpp_connection{}) -> ok.
unthrottled(UUID, Conn) ->
    gen_server:cast(?pid(UUID), {unthrottled, Conn}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([Gateway, ClientSupSup]) ->
    #gateway{uuid = UUID, name = Name, rps = RPS, settings = Settings,
             connections = Connections} = Gateway,
    ?log_info("Gateway #~s#: initializing smpp clients", [Name]),
    gproc:add_local_name(?name(UUID)),
    St = start_clients(#st{uuid = UUID, name = Name, rps = RPS,
                           settings = Settings, all = Connections,
                           sup = ClientSupSup, clients = ets:new(clients, [])}),
    update_scheduler(St),
    ?log_info("Gateway #~s#: initialized smpp clients", [Name]),
    {ok, St}.

terminate(_Reason, _St) ->
    ok.

handle_call(stop, _From, St) ->
    ?log_info("Gateway #~s#: stopping smpp clients", [St#st.name]),
    [ stop_client(Pid) || {Pid, _Conn} <- ets:tab2list(St#st.clients) ],
    ?log_info("Gateway #~s#: stopped smpp clients", [St#st.name]),
    {stop, normal, ok, St};

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast({throttled, Conn}, St) ->
    St1 = St#st{throttled = [Conn|St#st.throttled]},
    update_scheduler(St1),
    {noreply, St1};

handle_cast({unthrottled, Conn}, St) ->
    St1 = St#st{throttled = St#st.throttled -- [Conn]},
    update_scheduler(St1),
    {noreply, St1};

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info({timeout, _TRef, {start, Conn, Delay}}, St) ->
    Description = just_smpp_client:to_list(Conn),
    ?log_info("Gateway #~s#: starting client ~s", [St#st.name, Description]),
    case start_client(Conn, St) of
        {ok, Pid} ->
            ?log_info("Gateway #~s#: started client ~s", [St#st.name, Description]),
            monitor(process, Pid),
            ets:insert(St#st.clients, {Pid, Conn}),
            St1 = St#st{on = [Conn|St#st.on], off = St#st.off -- [Conn]},
            update_scheduler(St1),
            {noreply, St1};
        {error, Reason} ->
            MaxDelay = just_settings:get(max_reconnect_delay, St#st.settings),
            NewDelay = erlang:min(erlang:max(1000, Delay * 2), MaxDelay),
            ?log_error("Gateway #~s#: could not start client ~s (~p), "
                       "will try again in ~w second(s)",
                       [St#st.name, Description, Reason, NewDelay div 1000]),
            erlang:start_timer(NewDelay, self(), {start, Conn, NewDelay}),
            {noreply, St}
    end;

handle_info({'DOWN', _MRef, process, Pid, Reason}, St) ->
    [{Pid, Conn}] = ets:lookup(St#st.clients, Pid),
    ets:delete(St#st.clients, Pid),
    Description = just_smpp_client:to_list(Conn),
    ?log_error("Gateway #~s#: client ~s failed (~p)",
               [St#st.name, Description, Reason]),
    erlang:start_timer(0, self(), {start, Conn, 0}),
    St1 = St#st{on = St#st.on -- [Conn], off = [Conn|St#st.off],
                throttled = St#st.throttled -- [Conn]},
    update_scheduler(St1),
    {noreply, St1};

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% private functions
%% -------------------------------------------------------------------------

active_transmitters(St) ->
    Conns = St#st.on -- St#st.throttled,
    % only count connected unthrottled transmitters and transceivers.
    length([ t || #smpp_connection{type = T} <- Conns, T =/= receiver ]).

active_window_size(St) ->
    active_transmitters(St) * just_settings:get(smpp_window_size, St#st.settings).

update_scheduler(St) ->
    just_scheduler:update(St#st.uuid, active_window_size(St)).

start_clients(St) ->
    start_clients(St#st.all, St).

start_clients([], St) ->
    St;
start_clients([Conn|Conns], St) ->
    Description = just_smpp_client:to_list(Conn),
    ?log_info("Gateway #~s#: starting client ~s", [St#st.name, Description]),
    case start_client(Conn, St) of
        {ok, Pid} ->
            ?log_info("Gateway #~s#: started client ~s", [St#st.name, Description]),
            monitor(process, Pid),
            ets:insert(St#st.clients, {Pid, Conn}),
            start_clients(Conns, St#st{on = [Conn|St#st.on]});
        {error, Reason} ->
            ?log_error("Gateway #~s#: could not start client ~s (~p), "
                       "will try again in 1 second(s)",
                       [St#st.name, Description, Reason]),
            erlang:start_timer(1000, self(), {start, Conn, 1000}),
            start_clients(Conns, St#st{off = [Conn|St#st.off]})
    end.

start_client(Conn, St) ->
    {ok, _Sup, Client} =
        just_smpp_client_sup_sup:start_smpp_client_sup(St#st.sup, Conn),
    case just_smpp_client:connect(Client) of
        ok ->
            try just_smpp_client:bind(Client) of
                {ok, _} ->
                    just_smpp_client:start_metronome(Client),
                    {ok, Client};
                {error, _} = Error ->
                    Error
            catch
                _:Reason ->
                    {error, Reason}
            end;
        {error, _} = Error ->
            Error
    end.

stop_client(Pid) ->
    try
        just_smpp_client:unbind(Pid),
        just_smpp_client:stop(Pid)
    catch
        _:_ -> ok
    end.
