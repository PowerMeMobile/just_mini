-module(just_request_pq_tests).

-include_lib("eunit/include/eunit.hrl").

in_undefined_customer_test() ->
    PQ = just_request_pq:new(fun(_) -> undefined end),
    ?assertEqual(false, just_request_pq:in({<<"c1">>, <<"r1">>, 1, {1, 0}}, PQ)).

out_early_test() ->
    PQ = just_request_pq:new(fun(<<"c1">>) -> 9 end),
    {true, PQ1} = just_request_pq:in({<<"c1">>, <<"r1">>, 1, {0, 100}}, PQ),
    ?assertEqual(empty, just_request_pq:out({0, 99}, PQ1)).

out_order_test() ->
    PQ = just_request_pq:new(fun(<<"c1">>) -> 9; (<<"c2">>) -> 0 end),
    C1 = <<"c1">>,
    C2 = <<"c2">>,
    R1 = {C1, <<"r1">>, 1, {0, 1}},
    R2 = {C1, <<"r2">>, 1, {0, 2}},
    R3 = {C1, <<"r3">>, 1, {0, 3}},
    R4 = {C1, <<"r4">>, 1, {0, 3}},
    R5 = {C2, <<"r5">>, 1, {0, 1}},
    R6 = {C2, <<"r6">>, 1, {0, 1}},
    R7 = {C2, <<"r7">>, 1, {0, 2}},
    R8 = {C2, <<"r8">>, 1, {0, 3}},
    {true, PQ1} = just_request_pq:in(R7, PQ),
    {true, PQ2} = just_request_pq:in(R5, PQ1),
    {true, PQ3} = just_request_pq:in(R1, PQ2),
    {true, PQ4} = just_request_pq:in(R3, PQ3),
    {true, PQ5} = just_request_pq:in(R8, PQ4),
    {true, PQ6} = just_request_pq:in(R6, PQ5),
    {true, PQ7} = just_request_pq:in(R4, PQ6),
    {true, PQ8} = just_request_pq:in(R2, PQ7),
    {R1, PQ9} = just_request_pq:out({0, 100}, PQ8),
    {R2, PQ10} = just_request_pq:out({0, 100}, PQ9),
    {R3, PQ11} = just_request_pq:out({0, 100}, PQ10),
    {R4, PQ12} = just_request_pq:out({0, 100}, PQ11),
    {R5, PQ13} = just_request_pq:out({0, 100}, PQ12),
    {R6, PQ14} = just_request_pq:out({0, 100}, PQ13),
    {R7, PQ15} = just_request_pq:out({0, 100}, PQ14),
    {R8, PQ16} = just_request_pq:out({0, 100}, PQ15),
    ?assertEqual(empty, just_request_pq:out({0, 100}, PQ16)).

throttle_test() ->
    PQ = just_request_pq:new(fun(<<"c1">>) -> 9 end),
    R1 = {<<"c1">>, <<"r1">>, 1, {0, 0}},
    R2 = {<<"c1">>, <<"r2">>, 1, {0, 1}},
    {true, PQ1} = just_request_pq:in(R1, PQ),
    ?assertMatch({R1, _}, just_request_pq:out({0, 1}, PQ1)),
    {true, PQ2} = just_request_pq:throttle(<<"c1">>, PQ1),
    ?assertEqual(false, just_request_pq:throttle(<<"c1">>, PQ2)),
    ?assertEqual(empty, just_request_pq:out({0, 1}, PQ2)),
    {true, PQ3} = just_request_pq:in(R2, PQ2),
    ?assertEqual(empty, just_request_pq:out({0, 2}, PQ3)),
    {true, PQ4} = just_request_pq:unthrottle(<<"c1">>, PQ3),
    ?assertEqual(false, just_request_pq:unthrottle(<<"c1">>, PQ4)),
    {R1, PQ5} = just_request_pq:out({0, 2}, PQ4),
    {R2, PQ6} = just_request_pq:out({0, 2}, PQ5),
    ?assertEqual(empty, just_request_pq:out({0, 2}, PQ6)).

remove_test() ->
    Prios = ets:new(prios, []),
    F = fun(C) ->
            case ets:lookup(Prios, C) of
                [{C, Prio}] -> Prio; [] -> undefined
            end
        end,
    ets:insert(Prios, {<<"c1">>, 0}),
    PQ = just_request_pq:new(F),
    R1 = {<<"c1">>, <<"r1">>, 1, {0, 0}},
    R2 = {<<"c1">>, <<"r2">>, 1, {0, 1}},
    ?assertEqual(false, just_request_pq:remove(<<"c1">>, PQ)),
    {true, PQ1} = just_request_pq:in(R1, PQ),
    {true, PQ2} = just_request_pq:in(R2, PQ1),
    ?assertMatch({true, _, [<<"r1">>, <<"r2">>]}, just_request_pq:remove(<<"c1">>, PQ2)),
    {true, PQ3} = just_request_pq:throttle(<<"c1">>, PQ2),
    ?assertMatch({true, _, [<<"r1">>, <<"r2">>]}, just_request_pq:remove(<<"c1">>, PQ3)).

update_test() ->
    Prios = ets:new(prios, []),
    F = fun(C) ->
            case ets:lookup(Prios, C) of
                [{C, Prio}] -> Prio; [] -> undefined
            end
        end,
    PQ = just_request_pq:new(F),
    R1 = {<<"c1">>, <<"r1">>, 1, {0, 0}},
    R2 = {<<"c1">>, <<"r2">>, 1, {0, 1}},
    ?assertEqual(false, just_request_pq:update(<<"c1">>, PQ)),
    ets:insert(Prios, {<<"c1">>, 0}),
    ?assertEqual(false, just_request_pq:update(<<"c1">>, PQ)),
    {true, PQ1} = just_request_pq:in(R1, PQ),
    {true, PQ2} = just_request_pq:in(R2, PQ1),
    ?assertEqual(false, just_request_pq:update(<<"c1">>, PQ2)),
    {true, PQ3} = just_request_pq:throttle(<<"c1">>, PQ2),
    ?assertEqual(false, just_request_pq:update(<<"c1">>, PQ3)),
    ets:insert(Prios, {<<"c1">>, 9}),
    ?assertMatch({true, _}, just_request_pq:update(<<"c1">>, PQ2)),
    ?assertMatch({true, _}, just_request_pq:update(<<"c1">>, PQ3)),
    ets:delete(Prios, <<"c1">>),
    ?assertMatch({true, _, [<<"r1">>, <<"r2">>]}, just_request_pq:update(<<"c1">>, PQ2)),
    ?assertMatch({true, _, [<<"r1">>, <<"r2">>]}, just_request_pq:update(<<"c1">>, PQ3)).
