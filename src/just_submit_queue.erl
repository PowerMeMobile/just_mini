-module(just_submit_queue).

-include("gateway.hrl").
-include_lib("alley_common/include/logging.hrl").

-define(name(UUID), {UUID, submit_queue}).
-define(pid(UUID), gproc:lookup_local_name(?name(UUID))).

-behaviour(gen_server).

%% API exports
-export([start_link/1]).
-export([stop/1, pid/1, in/3, in_r/3, out/2]).

%% gen_server exports
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-type expiry() :: just_time:precise_time().

%% wl - waiting list.
-record(st, {uuid :: binary(), name :: string(), queue = queue:new() :: queue(),
             wl = [] :: [{pid(), any()}]}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}) -> {ok, pid()}.
start_link(Gateway) ->
    gen_server:start_link(?MODULE, [Gateway], []).

-spec stop(binary()) -> ok.
stop(UUID) ->
    gen_server:call(?pid(UUID), stop, infinity).

-spec pid(binary()) -> pid().
pid(UUID) ->
    ?pid(UUID).

-spec in(pid(), any(), expiry()) -> ok.
in(Pid, Value, Expiry) ->
    gen_server:cast(Pid, {in, Value, Expiry}).

-spec in_r(pid(), any(), expiry()) -> ok.
in_r(Pid, Value, Expiry) ->
    gen_server:cast(Pid, {in_r, Value, Expiry}).

-spec out(pid(), any()) -> {value, any(), expiry()} | empty.
out(Pid, NotifyMsg) ->
    gen_server:call(Pid, {out, NotifyMsg}, infinity).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([Gateway]) ->
    #gateway{uuid = UUID, name = Name} = Gateway,
    ?log_info("Gateway #~s#: initializing submit queue", [Name]),
    gproc:add_local_name(?name(UUID)),
    ?log_info("Gateway #~s#: initialized submit queue", [Name]),
    {ok, #st{uuid = UUID, name = Name}}.

terminate(_Reason, _St) ->
    ok.

handle_call(stop, _From, St) ->
    ?log_info("Gateway #~s#: stopped submit queue", [St#st.name]),
    {stop, normal, ok, St};

handle_call({out, NotifyMsg}, {Pid, _Tag}, St) ->
    case queue_out(St#st.queue) of
        {{value, {Value, Expiry}}, Queue} ->
            {reply, {value, Value, Expiry}, St#st{queue = Queue}};
        {empty, Queue} ->
            WL = [{Pid, NotifyMsg}|St#st.wl],
            {reply, empty, St#st{queue = Queue, wl = WL}}
    end;

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast({Op, Value, Expiry}, St) when Op =:= in; Op =:= in_r ->
    Queue = queue:Op({Value, Expiry}, St#st.queue),
    [ Pid ! NotifyMsg || {Pid, NotifyMsg} <- St#st.wl ],
    {noreply, St#st{queue = Queue, wl = []}};

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% private functions
%% -------------------------------------------------------------------------

queue_out(Q) ->
    queue_out(Q, just_time:precise_time()).

queue_out(Q, T) ->
    case queue:out(Q) of
        {{value, {_Value, Expiry}}, _Q1} = Result when Expiry > T ->
            Result;
        {{value, _}, Q1} ->
            queue_out(Q1, T);
        Empty ->
            Empty
    end.
