-module(just_request_pq_tests).

-include_lib("eunit/include/eunit.hrl").

empty_test() ->
    ?assertEqual(none, just_request_pq:out(just_calendar:precise_time(),
                                           just_request_pq:new())).
