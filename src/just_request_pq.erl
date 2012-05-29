%%% @doc A data structure for the scheduler.
%%% Holds all the customers' requests belonging to a particular gateway.
%%% There are ten (0..9) possible customer priorities.
%%% Requests are ordered by [CustomerPriority desc, AttemptTime asc].
-module(just_request_pq).

%% API exports.
-export([new/1, in/2, out/2, throttle/2, unthrottle/2, update/2, remove/2]).

-type priority_fun() :: fun((binary()) -> 0..9 | undefined).
-type request() :: {binary(), binary(), pos_integer(), just_calendar:precise_time()}.

-record(pq, {pfun :: priority_fun(), active :: tuple(),
             index = dict:new() :: dict(),
             throttled = dict:new() :: dict()}).

-opaque request_pq() :: #pq{}.
-export_type([request_pq/0]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

%% @doc Returns an empty priority queue.
-spec new(priority_fun()) -> request_pq().
new(PriorityFun) ->
    #pq{pfun = PriorityFun, active = erlang:make_tuple(10, gb_trees:empty())}.

%% @doc Inserts a request into the priority queue.
%% Returns 'false' if the customer doesn't exist.
-spec in(request(), request_pq()) -> false | {true, request_pq()}.
in({Customer, _Request, _Size, _AttemptTime} = R, PQ) ->
    case index_lookup(Customer, PQ) of
        {active, Prio, PrevAttemptTime} ->
            {true, in_active(R, Prio, PrevAttemptTime, PQ)};
        {throttled, _Prio} ->
            {true, in_throttled(R, PQ)};
        undefined ->
            case (PQ#pq.pfun)(Customer) of
                undefined -> false;
                Prio      -> {true, in_new_active(R, Prio, PQ)}
            end
    end.

%% @doc Takes out next available request from the priority queue.
%% Returns 'empty' if there is no ready request in the queue.
-spec out(just_calendar:precise_time(), request_pq()) ->
    empty | {request(), request_pq()}.
out(TimeNow, PQ) ->
    out(TimeNow, PQ, 9).

%% @doc Temporarily deactivates a customer. While the customer
%% is being throttled, its requests are not being dequeued.
%% Returns 'false' if there is no such customer in the queue or
%% if it's already being throttled.
-spec throttle(binary(), request_pq()) -> false | {true, request_pq()}.
throttle(Customer, PQ) ->
    case index_lookup(Customer, PQ) of
        {active, Prio, AttemptTime} ->
            Customers = get_active(Prio, PQ),
            Requests = gb_trees:get({AttemptTime, Customer}, Customers),
            Customers1 = gb_trees:delete({AttemptTime, Customer}, Customers),
            {true, PQ#pq{index = dict:store(Customer, {throttled, Prio}, PQ#pq.index),
                         active = replace_active(Prio, Customers1, PQ),
                         throttled = dict:store(Customer, Requests, PQ#pq.throttled)}};
        _ ->
            false
    end.

%% @doc Activates a throttled customer.
%% Returns 'false' if there is no such customer in the queue or
%% if it's not being throttled.
-spec unthrottle(binary(), request_pq()) -> false | {true, request_pq()}.
unthrottle(Customer, PQ) ->
    case index_lookup(Customer, PQ) of
        {throttled, Prio} ->
            Requests = dict:fetch(Customer, PQ#pq.throttled),
            {{AttemptTime, _}, _} = gb_trees:smallest(Requests),
            F = fun(Customers) ->
                    gb_trees:insert({AttemptTime, Customer}, Requests, Customers)
                end,
            {true, PQ#pq{index = dict:store(Customer, {active, Prio, AttemptTime},
                                            PQ#pq.index),
                         active = update_active(Prio, F, PQ),
                         throttled = dict:erase(Customer, PQ#pq.throttled)}};
        _ ->
            false
    end.

%% @doc Updates priority of a customer.
%% Returns 'false' if there is no such customer in the queue or
%% if its priority hasn't changed.
%% Returns {true, request_pq()} if the priority has changed.
%% Returns {true, request_pq(), Requests::binary()} if the customer
%% is no longer available, where Requests is a list of request UUIDs
%% that belonged to the customer.
-spec update(binary(), request_pq()) ->
    false | {true, request_pq()} | {true, request_pq(), [binary()]}.
update(Customer, PQ) ->
    case (PQ#pq.pfun)(Customer) of
        undefined ->
            remove(Customer, PQ);
        Prio ->
            case index_lookup(Customer, PQ) of
                undefined ->
                    false;
                {throttled, Prio} ->
                    false;
                {active, Prio, _AttemptTime} ->
                    false;
                Other ->
                    {true, move(Customer, Prio, Other, PQ)}
            end
    end.

%% @doc Removes all the customer's requests from the queue.
%% Returns 'false' if there is no such customer in the queue.
%% Returns {true, request_pq(), Requests::binary()} otherwise,
%% where Requests is a list of request UUIDs that belonged to the customer.
-spec remove(binary(), request_pq()) -> false | {true, request_pq(), [binary()]}.
remove(Customer, PQ) ->
    case index_lookup(Customer, PQ) of
        {throttled, _Prio} ->
            {true,
             PQ#pq{index = dict:erase(Customer, PQ#pq.index),
                   throttled = dict:erase(Customer, PQ#pq.throttled)},
             customer_requests(dict:fetch(Customer, PQ#pq.throttled))};
        {active, Prio, AttemptTime} ->
            Customers = get_active(Prio, PQ),
            Customers1 = gb_trees:delete({AttemptTime, Customer}, Customers),
            {true,
             PQ#pq{index = dict:erase(Customer, PQ#pq.index),
                   active = replace_active(Prio, Customers1, PQ)},
             customer_requests(gb_trees:get({AttemptTime, Customer}, Customers))};
        undefined ->
            false
    end.

%% -------------------------------------------------------------------------
%% enqueue
%% -------------------------------------------------------------------------

in_active({Customer, Request, Size, AttemptTime}, Prio, PrevAttemptTime, PQ)
        when AttemptTime >= PrevAttemptTime ->
    F = fun(Customers) ->
            Requests = gb_trees:insert({AttemptTime, Request}, Size,
                                       gb_trees:get({PrevAttemptTime, Customer},
                                                    Customers)),
            gb_trees:update({PrevAttemptTime, Customer}, Requests, Customers)
        end,
    PQ#pq{active = update_active(Prio, F, PQ)};

in_active({Customer, Request, Size, AttemptTime}, Prio, PrevAttemptTime, PQ) ->
    F = fun(Customers) ->
            Requests = gb_trees:insert({AttemptTime, Request}, Size,
                                       gb_trees:get({PrevAttemptTime, Customer},
                                                    Customers)),
            gb_trees:insert({AttemptTime, Customer}, Requests,
                            gb_trees:delete({PrevAttemptTime, Customer}, Customers))
        end,
     PQ#pq{index = dict:store(Customer, {active, Prio, AttemptTime}, PQ#pq.index),
           active = update_active(Prio, F, PQ)}.

in_throttled({Customer, Request, Size, AttemptTime}, PQ) ->
    F = fun(Requests) ->
            gb_trees:insert({AttemptTime, Request}, Size, Requests)
        end,
    PQ#pq{throttled = dict:update(Customer, F, PQ#pq.throttled)}.

in_new_active({Customer, Request, Size, AttemptTime}, Prio, PQ) ->
    Requests = gb_trees:insert({AttemptTime, Request}, Size,
                               gb_trees:empty()),
    F = fun(Customers) ->
            gb_trees:insert({AttemptTime, Customer}, Requests, Customers)
        end,
    PQ#pq{index = dict:store(Customer, {active, Prio, AttemptTime}, PQ#pq.index),
          active = update_active(Prio, F, PQ)}.

%% -------------------------------------------------------------------------
%% dequeue
%% -------------------------------------------------------------------------

out(_TimeNow, _PQ, -1) ->
    empty;
out(TimeNow, PQ, Prio) ->
    Customers = get_active(Prio, PQ),
    case gb_trees:is_empty(Customers) of
        true ->
            out(TimeNow, PQ, Prio - 1);
        false ->
            {{AttemptTime, Customer}, Requests} = gb_trees:smallest(Customers),
            case AttemptTime > TimeNow of
                true  -> out(TimeNow, PQ, Prio - 1);
                false -> out(Customer, Prio, Requests, PQ)
            end
    end.

out(Customer, Prio, Requests, PQ) ->
    {{AttemptTime, Request}, Size, Requests1} = gb_trees:take_smallest(Requests),
    Result = {Customer, Request, Size, AttemptTime},
    case gb_trees:is_empty(Requests1) of
        true ->
            F = fun(Customers) ->
                    gb_trees:delete({AttemptTime, Customer}, Customers)
                end,
            {Result, PQ#pq{index = dict:erase(Customer, PQ#pq.index),
                           active = update_active(Prio, F, PQ)}};
        false ->
            {{NextAttemptTime, _}, _} = gb_trees:smallest(Requests1),
            case NextAttemptTime =:= AttemptTime of
                true ->
                    F = fun(Customers) ->
                            gb_trees:update({AttemptTime, Customer}, Requests1,
                                            Customers)
                        end,
                    {Result, PQ#pq{active = update_active(Prio, F, PQ)}};
                false ->
                    F = fun(Customers) ->
                            gb_trees:insert({NextAttemptTime, Customer}, Requests1,
                                            gb_trees:delete({AttemptTime, Customer},
                                                            Customers)) end,
                    Index = dict:store(Customer, {active, Prio, NextAttemptTime},
                                       PQ#pq.index),
                    {Result, PQ#pq{index = Index,
                                   active = update_active(Prio, F, PQ)}}
            end
    end.

%% -------------------------------------------------------------------------
%% other private functions
%% -------------------------------------------------------------------------

get_active(Prio, PQ) ->
    element(Prio + 1, PQ#pq.active).

replace_active(Prio, Customers, PQ) ->
    setelement(Prio + 1, PQ#pq.active, Customers).

update_active(Prio, Fun, PQ) ->
    replace_active(Prio, Fun(get_active(Prio, PQ)), PQ).

index_lookup(Customer, PQ) ->
    case dict:find(Customer, PQ#pq.index) of
        {ok, Value} -> Value;
        error       -> undefined
    end.

customer_requests(Tree) ->
    [ Request || {_AttemptTime, Request} <- gb_trees:keys(Tree) ].

move(Customer, NewPrio, {throttled, _OldPrio}, PQ) ->
    PQ#pq{index = dict:store(Customer, {throttled, NewPrio}, PQ#pq.index)};
move(Customer, NewPrio, {active, OldPrio, AttemptTime}, PQ) ->
    Index = dict:store(Customer, {active, NewPrio, AttemptTime}, PQ#pq.index),
    OldCustomers = get_active(OldPrio, PQ),
    OldCustomers1 = gb_trees:delete({AttemptTime, Customer}, OldCustomers),
    Requests = gb_trees:get({AttemptTime, Customer}, OldCustomers),
    NewCustomers = get_active(NewPrio, PQ),
    NewCustomers1 = gb_trees:insert({AttemptTime, Customer}, Requests, NewCustomers),
    Active = setelement(NewPrio + 1,
                        setelement(OldPrio + 1, PQ#pq.active, OldCustomers1),
                        NewCustomers1),
    PQ#pq{index = Index, active = Active}.
