-module(just_customer).

-behaviour(gen_server).

%% API exports.
-export([start_link/2]).
-export([stop/1, request/3, update/2]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

%% wl - waiting list.
-record(st, {uuid :: binary(), rps :: pos_integer(),
             hg :: just_rps_histogram:histogram(),
             wl = [] :: [pid()], waiting = false :: boolean()}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(binary(), pos_integer()) -> {ok, pid()}.
start_link(UUID, RPS) ->
    gen_server:start_link(?MODULE, [UUID, RPS], []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:cast(Pid, stop).

-spec request(pid(), pos_integer(), just_time:precise_time()) ->
              boolean().
request(Pid, Reqs, Time) ->
    gen_server:call(Pid, {request, Reqs, Time}, infinity).

-spec update(pid(), pos_integer()) -> ok.
update(Pid, RPS) ->
    gen_server:cast(Pid, {update, RPS}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([UUID, RPS]) ->
    {ok, #st{uuid = UUID, rps = RPS, hg = just_rps_histogram:new()}, 1000}.

terminate(_Reason, _St) ->
    ok.

handle_call({request, _Reqs, _Time}, {Pid, _Tag}, #st{waiting = true} = St) ->
    {reply, false, St#st{wl = lists:usort([Pid|St#st.wl])}};

handle_call({request, Reqs, {S, MS}}, {Pid, _Tag}, St) ->
    case just_rps_histogram:sum(S - 1, MS, St#st.hg) < St#st.rps of
        true ->
            {reply, true, St#st{hg = just_rps_histogram:add(S, MS, Reqs, St#st.hg)},
             1000};
        false ->
            erlang:start_timer(50, self(), wait),
            {reply, false, St#st{wl = [Pid], waiting = true}}
    end;

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast({update, RPS}, St) ->
    Timeout = case St#st.waiting of
                  true  -> infinity;
                  false -> 1000
              end,
    {noreply, St#st{rps = RPS}, Timeout};

handle_cast(stop, St) ->
    {stop, normal, St};

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info({timeout, _TRef, wait}, St) ->
    {S, MS} = just_time:precise_time(),
    case just_rps_histogram:sum(S - 1, MS, St#st.hg) < St#st.rps of
        true ->
            [ Pid ! {customer_ready, St#st.uuid} || Pid <- St#st.wl ],
            {noreply, St#st{waiting = false, wl = []}, 1000};
        false ->
            erlang:start_timer(50, self(), wait),
            {noreply, St}
    end;

handle_info(timeout, St) ->
    {noreply, St#st{hg = just_rps_histogram:new()}, hibernate};

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.
