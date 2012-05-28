%%% @doc A data structure for the scheduler.
%%% Implemented as a tuple of 10 (1 for each priority) next-attempt-time
%%% ordered customer queues of next-attempt-time ordered request ids.

-module(just_request_pq).

%% API exports.
-export([new/0, in/6, out/2, move/4, take/3, return/4]).

-type slice() :: {dict(), gb_tree()}.

-opaque request_pq() :: {slice(), slice(), slice(), slice(), slice(),
                         slice(), slice(), slice(), slice(), slice()}.
-export_type([request_pq/0]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

%% @doc Returns an empty priority queue.
-spec new() -> request_pq().
new() ->
    erlang:make_tuple(10, {dict:new(), gb_trees:empty()}).

-spec in(0..9, binary(), binary(), just_calendar:precise_time(), any(),
         request_pq()) -> request_pq().
in(Prio, CustomerId, RequestId, AttemptAt, Meta, PQ) ->
    {Index, Customers} = slice(Prio, PQ),
    case dict:find(CustomerId, Index) of
        {ok, OldAttemptAt} ->
            Requests = gb_trees:get({OldAttemptAt, CustomerId}, Customers),
            NewRequests = gb_trees:insert({AttemptAt, RequestId}, Meta, Requests),
            NewAttemptAt = erlang:min(OldAttemptAt, AttemptAt),
            NewCustomers = gb_trees:insert({NewAttemptAt, CustomerId}, NewRequests,
                                           gb_trees:delete({OldAttemptAt, CustomerId},
                                                           Customers)),
            NewIndex = case NewAttemptAt of
                           OldAttemptAt -> Index;
                           _            -> dict:store(CustomerId, NewAttemptAt, Index)
                       end,
            replace_slice(Prio, {NewIndex, NewCustomers}, PQ);
        error ->
            NewIndex = dict:store(CustomerId, AttemptAt, Index),
            Requests = gb_trees:insert({AttemptAt, RequestId}, Meta, gb_trees:empty()),
            NewCustomers = gb_trees:insert({AttemptAt, CustomerId}, Requests, Customers),
            replace_slice(Prio, {NewIndex, NewCustomers}, PQ)
    end.

-spec out(just_calendar:precise_time(), request_pq()) ->
    none | {boolean(), {binary(), 0..9, binary(), any()}, request_pq()}.
out(Time, PQ) ->
    out(Time, PQ, 9).

out(_Time, _PQ, -1) ->
    none;
out(Time, PQ, Prio) ->
    {Index, Customers} = slice(Prio, PQ),
    case skim(Time, Customers) of
        none ->
            out(Time, PQ, Prio - 1);
        {AttemptAt, {CustomerId, Requests}} ->
            {{AttemptAt, RequestId}, Meta, NewRequests} =
                gb_trees:take_smallest(Requests),
            Result = {CustomerId, Prio, RequestId, Meta},
            case gb_trees:is_empty(NewRequests) of
                true ->
                    NewIndex = dict:erase(CustomerId, Index),
                    NewCustomers = gb_trees:delete({AttemptAt, CustomerId},
                                                   Customers),
                    {false, Result, replace_slice(Prio, {NewIndex, NewCustomers}, PQ)};
                false ->
                    {{NewAttemptAt, _}, _} = gb_trees:smallest(NewRequests),
                    NewIndex = dict:store(CustomerId, NewAttemptAt, Index),
                    NewCustomers =
                        case NewAttemptAt of
                            AttemptAt ->
                                gb_trees:update({AttemptAt, CustomerId},
                                                NewRequests, Customers);
                            _ ->
                                gb_trees:insert({NewAttemptAt, CustomerId},
                                                NewRequests,
                                                gb_trees:delete({AttemptAt, CustomerId},
                                                                Customers))
                    end,
                    {true, Result, replace_slice(Prio, {NewIndex, NewCustomers}, PQ)}
            end
    end.

-spec move(binary(), 0..9, 0..9, request_pq()) -> request_pq().
move(CustomerId, OrigPrio, DestPrio, PQ) ->
    {OrigIndex, OrigCustomers} = slice(OrigPrio, PQ),
    {DestIndex, DestCustomers} = slice(DestPrio, PQ),
    {ok, AttemptAt} = dict:find(CustomerId, OrigIndex),
    Requests = gb_trees:get({AttemptAt, CustomerId}, OrigCustomers),
    NewOrigSlice = {dict:erase(CustomerId, OrigIndex),
                    gb_trees:delete({AttemptAt, CustomerId}, OrigCustomers)},
    NewDestSlice = {dict:store(CustomerId, AttemptAt, DestIndex),
                    gb_trees:insert({AttemptAt, CustomerId}, Requests,
                                    DestCustomers)},
    replace_slice(DestPrio, NewDestSlice,
                  replace_slice(OrigPrio, NewOrigSlice, PQ)).

-spec take(binary(), 0..9, request_pq()) -> {gb_tree(), request_pq()}.
take(CustomerId, Prio, PQ) ->
    {Index, Customers} = slice(Prio, PQ),
    {ok, AttemptAt} = dict:find(CustomerId, Index),
    Requests = gb_trees:get({AttemptAt, CustomerId}, Customers),
    NewSlice = {dict:erase(CustomerId, Index),
                gb_trees:delete({AttemptAt, CustomerId}, Customers)},
    {Requests, replace_slice(Prio, NewSlice, PQ)}.

-spec return(binary(), 0..9, gb_tree(), request_pq()) -> request_pq().
return(CustomerId, Prio, Requests, PQ) ->
    {Index, Customers} = slice(Prio, PQ),
    {{AttemptAt, _}, _} = gb_trees:smallest(Requests),
    Slice = {dict:store(CustomerId, AttemptAt, Index),
             gb_trees:insert({AttemptAt, CustomerId}, Requests, Customers)},
    replace_slice(Prio, Slice, PQ).

%% -------------------------------------------------------------------------
%% private functions
%% -------------------------------------------------------------------------

slice(Prio, PQ) ->
    element(Prio + 1, PQ).

replace_slice(Prio, Slice, PQ) ->
    setelement(Prio + 1, PQ, Slice).

skim(Time, Customers) ->
    Iter = gb_trees:iterator(Customers),
    case gb_trees:next(Iter) of
        none ->
            none;
        {{AttemptAt, _}, _, _} when AttemptAt > Time ->
            none;
        {{AttemptAt, CustomerId}, Requests, Iter2} ->
            case skim(AttemptAt, Iter2, [{CustomerId, Requests}]) of
                [{CustomerId, Requests} = Tuple] ->
                    {AttemptAt, Tuple};
                Tuples ->
                    {AttemptAt, lists:nth(random:uniform(length(Tuples)), Tuples)}
            end
    end.

skim(AttemptAt, Iter, Acc) ->
    case gb_trees:next(Iter) of
        {{AttemptAt, CustomerId}, Requests, Iter2} ->
            skim(AttemptAt, Iter2, [{CustomerId, Requests}|Acc]);
        _ ->
            % none or larger AttemptAt
            Acc
    end.
