-module(just_worker_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("oserl/include/oserl.hrl").

-define(ERROR_TIMEOUT,  16#000000400).
-define(ERROR_CLOSED,   16#000000401).
-define(ERROR_EXPIRED,  16#000000402).
-define(ERROR_CUSTOMER, 16#000000403).
-define(ERROR_BLOCKED,  16#000000404).

preprocess_reply_test() ->
    DefaultSettings = [{terminal_errors, []}, {discarded_errors, []}],
    Fun = fun(Reply, Settings) ->
        just_worker:preprocess_reply(Reply, Settings)
    end,
    ?assertEqual({ok, 1}, Fun({ok, [{message_id, 1}]}, DefaultSettings)),
    ?assertEqual({error, ?ERROR_TIMEOUT, 0}, Fun(undefined, DefaultSettings)),
    ?assertEqual({error, ?ERROR_CLOSED, 0}, Fun({error, closed}, DefaultSettings)),
    ?assertEqual({error, ?ERROR_TIMEOUT, 0}, Fun({error, timeout}, DefaultSettings)),

    %% test ESME_RTHROTTLED
    ?assertEqual({error, ?ESME_RTHROTTLED, 0}, Fun({error, ?ESME_RTHROTTLED}, [
        {terminal_errors, [?ESME_RTHROTTLED]}, {discarded_errors, [?ESME_RTHROTTLED]}])),
    ?assertEqual({error, ?ESME_RTHROTTLED, 0}, Fun({error, ?ESME_RTHROTTLED}, [
        {terminal_errors, [?ESME_RTHROTTLED]}, {discarded_errors, [?ESME_RTHROTTLED]}])),
    ?assertEqual({error, ?ESME_RTHROTTLED, 0}, Fun({error, ?ESME_RTHROTTLED}, [
        {terminal_errors, [?ESME_RTHROTTLED]}, {discarded_errors, []}])),
    ?assertEqual({error, ?ESME_RTHROTTLED, 0}, Fun({error, ?ESME_RTHROTTLED}, [
        {terminal_errors, []}, {discarded_errors, []}])),


    %% test custom codes
    CustomError = custom_error,
    ?assertEqual({error, CustomError, infinity}, Fun({error, CustomError}, [
        {terminal_errors, [CustomError]}, {discarded_errors, []}])),
    ?assertEqual({error, CustomError, 0}, Fun({error, CustomError}, [
        {terminal_errors, []}, {discarded_errors, [CustomError]}])),
    ?assertEqual({error, CustomError, infinity}, Fun({error, CustomError}, [
        {terminal_errors, [CustomError]}, {discarded_errors, [CustomError]}])),
    ?assertEqual({error, CustomError, 1}, Fun({error, CustomError}, [
        {terminal_errors, []}, {discarded_errors, []}])),

    %% test undefined reply (timeout error)
    ?assertEqual({error, ?ERROR_TIMEOUT, infinity}, Fun(undefined, [
        {terminal_errors, [?ERROR_TIMEOUT]}, {discarded_errors, []}])),
    ?assertEqual({error, ?ERROR_TIMEOUT, infinity}, Fun(undefined, [
        {terminal_errors, [?ERROR_TIMEOUT]}, {discarded_errors, [?ERROR_TIMEOUT]}])),
    ?assertEqual({error, ?ERROR_TIMEOUT, 0}, Fun(undefined, [
        {terminal_errors, []}, {discarded_errors, [?ERROR_TIMEOUT]}])),
    ?assertEqual({error, ?ERROR_TIMEOUT, 0}, Fun(undefined, [
        {terminal_errors, []}, {discarded_errors, []}])),

    %% test {error, timeout} reply
    ?assertEqual({error, ?ERROR_TIMEOUT, infinity}, Fun({error, timeout}, [
        {terminal_errors, [?ERROR_TIMEOUT]}, {discarded_errors, []}])),
    ?assertEqual({error, ?ERROR_TIMEOUT, infinity}, Fun({error, timeout}, [
        {terminal_errors, [?ERROR_TIMEOUT]}, {discarded_errors, [?ERROR_TIMEOUT]}])),
    ?assertEqual({error, ?ERROR_TIMEOUT, 0}, Fun({error, timeout}, [
        {terminal_errors, []}, {discarded_errors, [?ERROR_TIMEOUT]}])),
    ?assertEqual({error, ?ERROR_TIMEOUT, 0}, Fun({error, timeout}, [
        {terminal_errors, []}, {discarded_errors, []}])).
