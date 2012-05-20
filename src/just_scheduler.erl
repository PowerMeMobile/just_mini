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
-export([stop/1, update/3, notify/5]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-record(st, {uuid :: binary(),
             name :: string(),
             sup :: pid(),
             workers = gb_sets:new() :: gb_set(),
             rps = 0 :: non_neg_integer(),
             % combined window_size of all transmitting smpp connections.
             slots_total = 0 :: non_neg_integer(),
             % number of active workers.
             slots_taken = 0 :: non_neg_integer(),
             prios :: dict(),
             pq :: just_request_pq:request_pq(),
             kill_list :: list(),
             throttled = false :: false | {true, reference()},
             waiting = false :: false | {true, reference()},
             % throttled customers.
             inactive_customers = dict:new() :: dict(),
             hg :: just_rps_histogram:histogram()}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}, pid()) -> {ok, pid()}.
start_link(Gateway, WorkerSup) ->
    gen_server:start_link(?MODULE, [Gateway, WorkerSup], []).

-spec stop(binary()) -> ok.
stop(UUID) ->
    gen_server:call(?pid(UUID), stop, infinity).

%% @doc Notifies the scheduler of the changed RPS and window size.
%% This function is called by just_smpp_clients when a transmitting smpp
%% connection goes down or goes up or gets throttled or gets unthrottled.
-spec update(binary(), non_neg_integer(), non_neg_integer()) -> ok.
update(UUID, RPS, WindowSize) ->
    gen_server:cast(?pid(UUID), {update, RPS, WindowSize}).

-spec notify(binary(), binary(), binary(), pos_integer(),
             just_calendar:precise_time()) -> ok.
notify(UUID, Customer, Request, Size, AttemptAt) ->
    gen_server:cast(?pid(UUID), {notify, Customer, Request, Size, AttemptAt}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([Gateway, WorkerSup]) ->
    #gateway{uuid = UUID, name = Name} = Gateway,
    lager:info("Gateway #~s#: initializing scheduler", [Name]),
    gproc:add_local_name(?name(UUID)),
    {Prios, PQ, KillList} = init_state(just_cabinets:table(UUID, request)),
    % TODO: log info about the loaded requests.
    St = #st{uuid = UUID, name = Name, sup = WorkerSup, prios = Prios, pq = PQ,
             kill_list = KillList, hg = just_rps_histogram:new()},
    just_customers:subscribe(),
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
    case dict:find(Customer, St#st.prios) of
        {ok, Prio} ->
            case dict:find(Customer, St#st.inactive_customers) of
                {ok, Requests} ->
                    % the gateway is waiting for the customer to unblock.
                    Requests1 = gb_trees:insert({AttemptAt, Request}, Size,
                                                Requests),
                    Inactive = dict:store(Customer, Requests1,
                                          St#st.inactive_customers),
                    {noreply, St#st{inactive_customers = Inactive}};
                error ->
                    PQ = in(Prio, Customer, Request, AttemptAt, Size, St#st.pq),
                    {noreply, maybe_schedule(St#st{pq = PQ, waiting = false})}
            end;
        error ->
            case just_mib:customer(Customer) of
                undefined ->
                    {noreply,
                     maybe_schedule(St#st{kill_list = [Request|St#st.kill_list]})};
                #customer{priority = Prio} ->
                    Prios = dict:store(Customer, Prio, St#st.prios),
                    PQ = in(Prio, Customer, Request, AttemptAt, Size, St#st.pq),
                    {noreply, maybe_schedule(St#st{prios = Prios, pq = PQ,
                                                   waiting = false})}
            end
    end;

handle_cast({update, RPS, WindowSize}, St) ->
    {noreply, maybe_schedule(St#st{rps = RPS, slots_total = WindowSize,
                                   throttled = false})};

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info({customer_ready, UUID}, St) ->
    case dict:find(UUID, St#st.inactive_customers) of
        {ok, Requests} ->
            % still waiting for the customer.
            {ok, Prio} = dict:find(UUID, St#st.prios),
            PQ = just_request_pq:return(UUID, Prio, Requests, St#st.pq),
            Inactive = dict:erase(UUID, St#st.inactive_customers),
            {noreply, maybe_schedule(St#st{inactive_customers = Inactive,
                                           pq = PQ, waiting = false})};
        error ->
            % the customer had been removed.
            {noreply, St}
    end;

handle_info({customer_updated, UUID}, St) ->
    case just_mib:customer(UUID) of
        undefined ->
            % the customer was removed. will eventually clean up the pq
            % from inside do_schedule/5.
            {noreply, St};
        #customer{priority = Prio} ->
            % customer's priority or rps were changed. or nothing at all.
            case dict:find(UUID, St#st.prios) of
                {ok, Prio} ->
                    % priority hasn't changed, and we don't care about RPS.
                    {noreply, St};
                {ok, Prio1} ->
                    % priority has changed.
                    Prios = dict:store(UUID, Prio, St#st.prios),
                    case dict:find(UUID, St#st.inactive_customers) of
                        {ok, _} ->
                            % the customer is inactive, no need to change anything
                            % but the index.
                            {noreply, St#st{prios = Prios}};
                        error ->
                            % this customer has requests in the PQ.
                            PQ = just_request_pq:move(UUID, Prio1, Prio, St#st.pq),
                            {noreply, St#st{prios = Prios, pq = PQ}}
                    end;
                error ->
                    % the customer is irrelevant to this gateway
                    % (has no requests in the queue).
                    {noreply, St}
            end
    end;

%% a request processor exited.
handle_info({'DOWN', _MRef, process, Pid, normal}, St) ->
    Workers = gb_sets:delete(Pid, St#st.workers),
    Taken = St#st.slots_taken - 1,
    {noreply, maybe_schedule(St#st{workers = Workers, slots_taken = Taken})};

handle_info({timeout, TRef, unthrottle}, #st{throttled = {true, TRef}} = St) ->
    {noreply, maybe_schedule(St#st{throttled = false})};

handle_info({timeout, TRef, tryagain}, #st{waiting = {true, TRef}} = St) ->
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
    F = fun(Key, Value, {Prios, PQ, KillList}) ->
            Req = binary_to_term(Value),
            Customer = Req#request.customer,
            AttemptAt = Req#request.attempt_at,
            Size = length(Req#request.todo_segments),
            case dict:find(Customer, Prios) of
                {ok, Prio} ->
                    {Prios, in(Prio, Customer, Key, AttemptAt, Size, PQ), KillList};
                error ->
                    case just_mib:customer(Customer) of
                        undefined ->
                            {Prios, PQ, [Key|KillList]};
                        #customer{priority = Prio} ->
                            {dict:store(Customer, Prio, Prios),
                             in(Prio, Customer, Key, AttemptAt, Size, PQ),
                             KillList}
                    end
            end
        end,
    toke_drv:fold(F, {dict:new(), just_request_pq:new(), []}, Toke).

wait_for_workers(St) ->
    case gb_sets:is_empty(St#st.workers) of
        true ->
            St;
        false ->
            receive
                {'DOWN', _MRef, process, Pid, _Info} ->
                    Workers = gb_sets:delete_any(Pid, St#st.workers),
                    wait_for_workers(St#st{workers = Workers})
            end
    end.

%% -------------------------------------------------------------------------
%% scheduling
%% -------------------------------------------------------------------------

maybe_schedule(#st{kill_list = [Request|KL]} = St) ->
    Pid = just_worker_sup:start_worker(St#st.sup),
    monitor(process, Pid),
    just_worker:kill(Pid, Request),
    Workers = gb_sets:add(Pid, St#st.workers),
    Taken = St#st.slots_taken + 1,
    maybe_schedule(St#st{kill_list = KL, workers = Workers, slots_taken = Taken});
maybe_schedule(#st{slots_total = Total, slots_taken = Taken} = St)
        when Taken >= Total ->
    St;
maybe_schedule(#st{throttled = {true, _}} = St) ->
    St;
maybe_schedule(#st{waiting = {true, _}} = St) ->
    St;
maybe_schedule(St) ->
    {S, MS} = Time = just_calendar:precise_time(),
    case just_rps_histogram:sum(S - 1, MS, St#st.hg) >= St#st.rps of
        true ->
            TRef = erlang:start_timer(50, self(), unthrottle),
            St#st{throttled = {true, TRef}};
        false ->
            case out(Time, St#st.pq) of
                none ->
                    TRef = erlang:start_timer(50, self(), tryagain),
                    St#st{waiting = {true, TRef}};
                {AnyLeft, Result, PQ} ->
                    do_schedule(Time, AnyLeft, Result, PQ, St)
            end
    end.

%% there are slots available and the gateway isn't throttled,
%% there is at least one request in the PQ.
do_schedule(Time, AnyLeft, {Customer, Prio, Request, Size}, PQ, St) ->
    case just_customers:request(Customer, Size, Time) of
        true ->
            Pid = just_worker_sup:start_worker(St#st.sup),
            monitor(process, Pid),
            just_worker:work(Pid, Request),
            Workers = gb_sets:add(Pid, St#st.workers),
            Taken = St#st.slots_taken + 1,
            {S, MS} = Time,
            Prios = case AnyLeft of
                        true  -> St#st.prios;
                        false -> dict:erase(Customer, St#st.prios)
                    end,
            HG = just_rps_histogram:add(S, MS, Size, St#st.hg),
            maybe_schedule(St#st{workers = gb_sets:add(Pid, Workers), prios = Prios,
                                 slots_taken = Taken, hg = HG, pq = PQ});
        false ->
            % the customer is throttled now.
            {Requests, PQ1} = just_request_pq:take(Customer, Prio, St#st.pq),
            Inactive = dict:store(Customer, Requests, St#st.inactive_customers),
            maybe_schedule(St#st{inactive_customers = Inactive, pq = PQ1});
        undefined ->
            % the customer is no longer active (removed from the database).
            {Requests, PQ1} = just_request_pq:take(Customer, Prio, St#st.pq),
            maybe_schedule(St#st{prios = dict:erase(Customer, St#st.prios),
                                 kill_list = St#st.kill_list ++ ids(Requests),
                                 pq = PQ1})
    end.

%% -------------------------------------------------------------------------
%% other private functions
%% -------------------------------------------------------------------------

in(Prio, Customer, Request, AttemptAt, Size, PQ) ->
    just_request_pq:in(Prio, Customer, Request, AttemptAt, Size, PQ).

out(Time, PQ) ->
    just_request_pq:out(Time, PQ).

ids(Requests) ->
    ids(gb_trees:iterator(Requests), []).

ids(Iter, Acc) ->
    case gb_trees:next(Iter) of
        none ->
            Acc;
        {{_AttemptAt, RequestId}, _Size, Iter2} ->
            ids(Iter2, [RequestId|Acc])
    end.
