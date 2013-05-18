-module(just_throughput).

-behaviour(gen_server).

%% API exports
-export([start_link/0,
         incr/4,
         slices/0]).

%% gen_server exports
-export([init/1,
         terminate/2,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         code_change/3]).

-define(N_SLICES_TO_STORE, 600).

-type counter() ::
    {{Gtw :: string(), Conn :: string(), Type :: atom()}, N :: integer()}.

-record(slice, {key, counters = [] :: [counter()]}).
-record(st, {slices = [] :: [#slice{}]}).

%% -------------------------------------------------------------------------
%% API functions
%% -------------------------------------------------------------------------

-spec start_link/0 :: () -> {'ok', pid()} | 'ignore' | {'error', any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-type msg_type() :: sms_in | sms_out.
-spec incr/4 :: (string(), string(), msg_type(), pos_integer()) -> 'ok'.
incr(GtwID, GtwName, MsgType, Incr) ->
    gen_server:cast(?MODULE, {incr, GtwID, GtwName, MsgType, Incr}).

-spec slices/0 :: () -> list().
slices() ->
    gen_server:call(?MODULE, slices, infinity).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    lager:info("throughput counter: initializing"),
    {ok, #st{}}.

terminate(Reason, _St) ->
    lager:info("throughput counter: terminated (~p)", [Reason]).

handle_call(slices, _From, St) ->
    Reply =
        lists:map(
            fun(#slice{key = Key, counters = Counters}) ->
                    {Y, Mon, D, H, Min, S} = Key,
                    {
                        dh_date:format(
                            "ymdHis", {{Y, Mon, D}, {H, Min, S}}
                        ),
                        lists:map(
                            fun({{GtwID, GtwName, MsgType}, Count}) ->
                                    {GtwID, GtwName, MsgType, Count}
                            end, lists:sort(Counters)
                        )
                    }
            end, lists:reverse(St#st.slices)
        ),
    {reply, Reply, St}.

handle_cast({incr, GtwID, GtwName, MsgType, Incr}, St) ->
    Sig = {GtwID, GtwName, MsgType},
    Key = key(calendar:universal_time()),
    {H, T} =
        case St#st.slices of
            [#slice{key = Key} = S|Ss] -> {S, Ss};
            Ss                         -> {#slice{key = Key}, Ss}
        end,
    Counters =
        case lists:keytake(Sig, 1, H#slice.counters) of
            {value, {_, N}, Counters_} -> [{Sig, N + Incr}|Counters_];
            false                      -> [{Sig, Incr}|H#slice.counters]
        end,
    Cutoff =
        if
            length(T) > ?N_SLICES_TO_STORE ->
                lists:sublist(T, ?N_SLICES_TO_STORE);
            true -> T
        end,
    {noreply, St#st{slices = [H#slice{counters = Counters}|Cutoff]}}.

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% Private functions
%% -------------------------------------------------------------------------

key({{Y, Mon, D}, {H, Min, S}}) ->
    {Y, Mon, D, H, Min, S}.
