-module(just_rps_histogram_tests).

-include_lib("eunit/include/eunit.hrl").

-import(just_rps_histogram, [new/0, add/4, sum/3]).

add_sum_test() ->
    H0 = new(),
    ?assertEqual(0, sum(1, 0, H0)),
    H1 = add(1, 0, 2, H0),
    ?assertEqual(2, sum(1, 0, H1)),
    H2 = add(1, 100, 1, H1),
    ?assertEqual(3, sum(1, 0, H2)),
    ?assertEqual(1, sum(1, 100, H2)),
    H3 = add(1, 100, 3, H2),
    ?assertEqual(6, sum(1, 0, H3)),
    H4 = lists:foldl(fun(N, Acc) -> add(1, N * 50, N, Acc) end,
                     new(), lists:seq(0, 19)),
    ?assertEqual(190, sum(1, 0, H4)),
    H5 = lists:foldl(fun(N, Acc) -> add(2, N * 50, N, Acc) end,
                     H4, lists:seq(0, 19)),
    ?assertEqual(190, sum(2, 0, H5)),
    H6 = add(3, 100, 1, H5),
    ?assertEqual(190, sum(2, 100, H6)).
