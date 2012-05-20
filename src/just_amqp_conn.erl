-module(just_amqp_conn).

-include("gateway.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-define(name(UUID), {UUID, amqp_conn}).
-define(pid(UUID), gproc:lookup_local_name(?name(UUID))).

-behaviour(gen_server).

%% API exports.
-export([start_link/3]).
-export([stop/1, open_channel/1]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

%% wl - waiting list.
-record(st, {uuid :: binary(), name :: string(), sup :: pid(), conn :: pid(),
             wl = [] :: [pid()]}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}, pid(), pid()) -> {ok, pid()}.
start_link(Gateway, Sup, Conn) ->
    gen_server:start_link(?MODULE, [Gateway, Sup, Conn], []).

-spec stop(binary()) -> ok.
stop(UUID) ->
    gen_server:call(?pid(UUID), stop, infinity).

-spec open_channel(binary()) -> {ok, pid()} | unavailable.
open_channel(UUID) ->
    gen_server:call(?pid(UUID), open_channel, infinity).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([Gateway, Sup, Conn]) ->
    #gateway{uuid = UUID, name = Name} = Gateway,
    gproc:add_local_name(?name(UUID)),
    lager:info("Gateway #~s#: connecting to the AMQP broker", [Name]),
    case amqp_gen_connection:connect(Conn) of
        {ok, Conn} ->
            lager:info("Gateway #~s#: connected to the AMQP broker", [Name]),
            _MRef = monitor(process, Conn),
            {ok, #st{uuid = UUID, name = Name, sup = Sup, conn = Conn}};
        {error, Error} ->
            % econnrefused, auth_failure (bad username/password),
            % access_refused (wrong vhost).
            lager:error("Gateway #~s#: could not connect to the AMQP broker (~s), "
                        "will try again in 1 second(s)",
                        [Name, Error]),
            erlang:start_timer(1000, self(), {connect, 1000}),
            {ok, #st{uuid = UUID, name = Name, sup = Sup}}
    end.

terminate(_Reason, _St) ->
    ok.

handle_call(stop, _From, St) ->
    case St#st.conn of
        undefined ->
            ok;
        Conn ->
            lager:info("Gateway #~s#: disconnecting from the AMQP broker",
                       [St#st.name]),
            amqp_connection:close(Conn),
            lager:info("Gateway #~s#: disconnected from the AMQP broker",
                       [St#st.name]),
            % workaround for a supervisor issue that sometimes logs a shutdown_error if
            % a temporary child exits fractionally early.
            timer:sleep(50)
    end,
    {stop, normal, ok, St#st{conn = undefined}};

handle_call(open_channel, {Pid, _Tag}, #st{conn = undefined} = St) ->
    {reply, unavailable, St#st{wl = [Pid|St#st.wl]}};

handle_call(open_channel, {Pid, _Tag}, St) ->
    try amqp_connection:open_channel(St#st.conn) of
        {ok, Chan} ->
            {reply, {ok, Chan}, St};
        _ ->
            {reply, unavailable, St#st{wl = [Pid|St#st.wl]}}
    catch
        _:_ ->
            {reply, unavailable, St#st{wl = [Pid|St#st.wl]}}
    end;

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info({timeout, _TRef, {connect, Delay}}, St) ->
    {ok, _ConnSup, Conn} = just_amqp_conn_sup:start_amqp_connection_sup(St#st.sup),
    lager:info("Gateway #~s#: connecting to the AMQP broker", [St#st.name]),
    case amqp_gen_connection:connect(Conn) of
        {ok, Conn} ->
            lager:info("Gateway #~s#: connected to the AMQP broker", [St#st.name]),
            _MRef = monitor(process, Conn),
            [ P ! amqp_conn_ready || P <- St#st.wl ],
            {noreply, St#st{conn = Conn, wl = []}};
        {error, Error} ->
            % econnrefused, auth_failure (bad username/password),
            % access_refused (wrong vhost).
            NewDelay = erlang:min(erlang:max(1000, Delay * 2), 30000),
            lager:error("Gateway #~s#: could not connect to the AMQP broker (~s), "
                        "will try again in ~w second(s)",
                        [St#st.name, Error, NewDelay div 1000]),
            erlang:start_timer(NewDelay, self(), {connect, NewDelay}),
            {noreply, St}
    end;

handle_info({'DOWN', _MRef, process, _Conn, Info}, St) ->
    lager:error("Gateway #~s#: AMQP broker connection failed (~w)",
                [St#st.name, Info]),
    erlang:start_timer(0, self(), {connect, 0}),
    {noreply, St#st{conn = undefined}};

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.
