-module(just_time).

-export([unix_time/0,
         precise_time/0,
         datetime_to_unix_time/1,
         unix_time_to_datetime/1,
         unix_time_to_utc_string/1]).

-type unix_time() :: integer().
-type milliseconds() :: 0..999.
-type precise_time() :: {unix_time(), milliseconds()}.

-export_type([unix_time/0, milliseconds/0, precise_time/0]).

-define(EPOCH, 62167219200).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec unix_time() -> unix_time().
unix_time() ->
    datetime_to_unix_time(erlang:universaltime()).

-spec precise_time() -> precise_time().
precise_time() ->
    {_MegaSecs, _Secs, MicroSecs} = TS = os:timestamp(),
    {datetime_to_unix_time(calendar:now_to_universal_time(TS)), MicroSecs div 1000}.

-spec datetime_to_unix_time(calendar:datetime()) -> unix_time().
datetime_to_unix_time(DateTime) ->
    calendar:datetime_to_gregorian_seconds(DateTime) - ?EPOCH.

-spec unix_time_to_datetime(unix_time()) -> calendar:datetime().
unix_time_to_datetime(UnixTime) ->
    calendar:gregorian_seconds_to_datetime(UnixTime + ?EPOCH).

-spec unix_time_to_utc_string(unix_time()) -> string().
unix_time_to_utc_string(UnixTime) ->
    {{Y, Mon, D}, {H, Min, S}} = unix_time_to_datetime(UnixTime),
    lists:concat([pad(Y rem 100), pad(Mon), pad(D), pad(H), pad(Min), pad(S)]).

%% -------------------------------------------------------------------------
%% private functions
%% -------------------------------------------------------------------------

pad(N) when N > 9 ->
    N;
pad(N) ->
    [$0, N + 48].
