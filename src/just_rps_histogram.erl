%%% @doc A data structure to represent one second's worth of requests.
%%% Used to implement customer and gateway throttling.

-module(just_rps_histogram).

-export([new/0, add/4, sum/3]).

-opaque histogram() :: {list(), list()}.
-type seconds() :: just_calendar:unix_time().
-type milliseconds() :: just_calendar:milliseconds().

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

%% @doc Returns an empty histogram.
-spec new() -> histogram().
new() ->
    {[], []}.

%% @doc Returns an updated histogram with N added to the right interval.
-spec add(seconds(), milliseconds(), pos_integer(), histogram()) -> histogram().
add(SecondsNow, MillisecondsNow, N, {Front, Back}) ->
    K = {SecondsNow, MillisecondsNow div 20},
    case Front of
        [] ->
            {[{K, N}], Back};
        [{K, PrevN}|T] ->
            {[{K, PrevN + N}|T], Back};
        _ ->
            case length(Front) < 20 of
                true  -> {[{K, N}|Front], Back};
                false -> {[{K, N}], Front}
            end
    end.

%% @doc Returns the sum of all intervals since provided time.
-spec sum(seconds(), milliseconds(), histogram()) -> non_neg_integer().
sum(SecondsSince, MillisecondsSince, {Front, Back}) ->
    sum({SecondsSince, MillisecondsSince div 20}, Front, Back, 0).

%% -------------------------------------------------------------------------
%% private functions
%% -------------------------------------------------------------------------

sum(_Key, [], [], Acc) ->
    Acc;
sum(Key, [{K, N}|T], Back, Acc) when K >= Key ->
    sum(Key, T, Back, Acc + N);
sum(_Key, [_|_], _Back, Acc) ->
    Acc;
sum(Key, [], [{K, N}|T], Acc) when K >= Key ->
    sum(Key, [], T, Acc + N);
sum(_Key, [], [_|_], Acc) ->
    Acc.
