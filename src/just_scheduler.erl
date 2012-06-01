%%% TODO: logging.
-module(just_scheduler).

-include("gateway.hrl").
-include("customer.hrl").
-include("persistence.hrl").

-define(name(UUID), {UUID, scheduler}).
-define(pid(UUID), gproc:lookup_local_name(?name(UUID))).

-behaviour(gen_server).

%% API exports.
-export([start_link/2]).
-export([stop/1, update/2, notify/5]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-record(st, {uuid :: binary(),
             name :: string(),
             sup :: pid(),
             workers :: ets:tid(),
             % combined window_size of all transmitting smpp connections.
             slots_total = 0 :: non_neg_integer(),
             % number of active workers (excluding the killing ones).
             slots_taken = 0 :: non_neg_integer(),
             pq :: just_request_pq:request_pq(),
             waiting = false :: false | {true, reference()}}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}, pid()) -> {ok, pid()}.
start_link(Gateway, WorkerSup) ->
    gen_server:start_link(?MODULE, [Gateway, WorkerSup], []).

-spec stop(binary()) -> ok.
stop(UUID) ->
    gen_server:call(?pid(UUID), stop, infinity).

%% @doc Notifies the scheduler of the changed window size.
%% This function is called by just_smpp_clients when a transmitting smpp
%% connection goes down or goes up or gets throttled or gets unthrottled.
-spec update(binary(), non_neg_integer()) -> ok.
update(UUID, WindowSize) ->
    gen_server:cast(?pid(UUID), {update, WindowSize}).

-spec notify(binary(), binary(), binary(), pos_integer(),
             just_time:precise_time()) -> ok.
notify(UUID, Customer, Request, Size, AttemptAt) ->
    gen_server:cast(?pid(UUID), {notify, Customer, Request, Size, AttemptAt}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([Gateway, WorkerSup]) ->
    #gateway{uuid = UUID, name = Name} = Gateway,
    lager:info("Gateway #~s#: initializing scheduler", [Name]),
    gproc:add_local_name(?name(UUID)),
    just_customers:subscribe(), % FIXME: use gen_event.
    {PQ, Size, KillList} = init_state(just_cabinets:table(UUID, request)),
    lager:info("Gateway #~s#: ~w requests in the queue, ~w by undefined customers",
               [Name, Size, length(KillList)]),
    St = kill(KillList, #st{uuid = UUID, name = Name, sup = WorkerSup, pq = PQ,
                            workers = ets:new(workers, [])}),
    lager:info("Gateway #~s#: initialized scheduler", [Name]),
    {ok, maybe_schedule(St)}.

terminate(_Reason, _St) ->
    ok.

handle_call(stop, _From, St) ->
    lager:info("Gateway #~s#: stopping scheduler", [St#st.name]),
    St1 = wait_for_workers(St),
    lager:info("Gateway #~s#: stopped scheduler", [St#st.name]),
    {stop, normal, ok, St1};

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast({notify, Customer, Request, Size, AttemptAt}, St) ->
    case just_request_pq:in({Customer, Request, Size, AttemptAt}, St#st.pq) of
        {true, PQ} ->
            {noreply, maybe_schedule(St#st{pq = PQ, waiting = false})};
        false ->
            {noreply, kill(Request, St)}
    end;

handle_cast({update, WindowSize}, St) ->
    {noreply, maybe_schedule(St#st{slots_total = WindowSize})};

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info({customer_ready, UUID}, St) ->
    case just_request_pq:unthrottle(UUID, St#st.pq) of
        {true, PQ} ->
            {noreply, maybe_schedule(St#st{pq = PQ, waiting = false})};
        false ->
            {noreply, St}
    end;

handle_info({customer_updated, UUID}, St) ->
    case just_request_pq:update(UUID, St#st.pq) of
        false ->
            {noreply, St};
        {true, PQ} ->
            {noreply, St#st{pq = PQ}};
        {true, PQ, Requests} ->
            {noreply, kill(Requests, St#st{pq = PQ})}
    end;

%% a request processor exited.
handle_info({'DOWN', _MRef, process, Pid, normal}, St) ->
    [{Pid, Op}] = ets:lookup(St#st.workers, Pid),
    ets:delete(St#st.workers, Pid),
    Taken = St#st.slots_taken - case Op of work -> 1; kill -> 0 end,
    {noreply, maybe_schedule(St#st{slots_taken = Taken})};

handle_info({timeout, TRef, schedule}, #st{waiting = {true, TRef}} = St) ->
    {noreply, maybe_schedule(St#st{waiting = false})};

%% a cancelled timer.
handle_info({timeout, _TRef, _}, St) ->
    {noreply, St};

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% init and terminate helpers
%% -------------------------------------------------------------------------

init_state(Toke) ->
    F = fun(Key, Value, {PQ, Total, KillList}) ->
            % Key is request UUID, Value is term_to_binary'd #request{}.
            Req = binary_to_term(Value),
            Customer = Req#request.customer,
            AttemptAt = Req#request.attempt_at,
            Size = length(Req#request.todo_segments),
            case just_request_pq:in({Customer, Key, Size, AttemptAt}, PQ) of
                {true, PQ1} -> {PQ1, Total + Size, KillList};
                false       -> {PQ, Total, [Key|KillList]}
            end
    end,
    P = fun(Customer) ->
            case just_mib:customer(Customer) of
                #customer{priority = Prio} -> Prio;
                undefined                  -> undefined
            end
        end,
    toke_drv:fold(F, {just_request_pq:new(P), 0, []}, Toke).

wait_for_workers(St) ->
    wait_for_workers(ets:info(St#st.workers, size), St).

wait_for_workers(0, St) ->
    St;
wait_for_workers(N, St) ->
    receive
        {'DOWN', _MRef, process, Pid, _Info} ->
            ets:delete(St#st.workers, Pid),
            wait_for_workers(N - 1, St)
    end.

%% -------------------------------------------------------------------------
%% scheduling
%% -------------------------------------------------------------------------

maybe_schedule(#st{slots_total = Total, slots_taken = Taken} = St) when Taken >= Total ->
    St;
maybe_schedule(#st{waiting = {true, _}} = St) ->
    St;
maybe_schedule(St) ->
    Time = just_time:precise_time(),
    case just_request_pq:out(Time, St#st.pq) of
        empty ->
            TRef = erlang:start_timer(50, self(), schedule),
            St#st{waiting = {true, TRef}};
        {Result, PQ} ->
            do_schedule(Time, Result, PQ, St)
    end.

%% there are slots available and there is at least one request in the PQ.
do_schedule(Time, {Customer, Request, Size, _AttemptAt}, PQ, St) ->
    case just_customers:request(Customer, Size, Time) of
        true ->
            maybe_schedule(start_worker(work, Request, St#st{pq = PQ}));
        false ->
            % the customer is throttled now.
            {true, PQ1} = just_request_pq:throttle(Customer, St#st.pq),
            maybe_schedule(St#st{pq = PQ1});
        undefined ->
            % the customer is no longer active (removed from the database).
            {true, PQ1, Requests} = just_request_pq:remove(Customer, St#st.pq),
            maybe_schedule(kill(Requests, St#st{pq = PQ1}))
    end.

-spec start_worker(work | kill, binary(), #st{}) -> #st{}.
start_worker(Op, Request, St) ->
    Pid = just_worker_sup:start_worker(St#st.sup),
    monitor(process, Pid),
    just_worker:Op(Pid, Request),
    ets:insert(St#st.workers, {Pid, Op}),
    St#st{slots_taken = St#st.slots_taken + case Op of work -> 1; kill -> 0 end}.

kill(Requests, St) when is_list(Requests) ->
    lists:foldl(fun(R, State) -> kill(R, State) end, St, Requests);
kill(Request, St) when is_binary(Request) ->
    start_worker(kill, Request, St).
