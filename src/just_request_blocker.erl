-module(just_request_blocker).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    block/1,
    unblock/1,
    is_blocked/1
]).

%% Support API
-export([
    fetch_all/0,
    remove_all/0
]).

%% gen_server exports
-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    code_change/3
]).

-include_lib("alley_common/include/logging.hrl").

-define(TCH_FILE, "data/tokyo/reqblock.tch").

-record(st, {
    toke :: pid(),
    index :: gb_tree(),
    size :: non_neg_integer(),
    max :: non_neg_integer()
}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec block(binary()) -> ok.
block(BatchUuid) ->
    gen_server:call(?MODULE, {block, BatchUuid}, infinity).

-spec unblock(binary()) -> ok.
unblock(BatchUuid) ->
    gen_server:call(?MODULE, {unblock, BatchUuid}, infinity).

-spec is_blocked(binary()) -> boolean().
is_blocked(BatchUuid) ->
    gen_server:call(?MODULE, {is_blocked, BatchUuid}, infinity).

%% -------------------------------------------------------------------------
%% Support API
%% -------------------------------------------------------------------------

-spec fetch_all() -> [binary()].
fetch_all() ->
    gen_server:call(?MODULE, fetch_all).

-spec remove_all() -> ok.
remove_all() ->
    gen_server:cast(?MODULE, remove_all).

%% -------------------------------------------------------------------------
%% gen_server callbacks
%% -------------------------------------------------------------------------

init([]) ->
    process_flag(trap_exit, true),
    ?log_info("Request blocker: initializing", []),
    {ok, Toke} = toke_drv:start_link(),
    ok = init_cabinet(Toke, ?TCH_FILE),
    {Index, Size} = init_state(Toke),
    {ok, cut_overhead(
        #st{toke = Toke, index = Index, size = Size, max = 1000})}.

terminate(Reason, St) ->
    toke_drv:close(St#st.toke),
    toke_drv:delete(St#st.toke),
    toke_drv:stop(St#st.toke),
    ?log_info("Request blocker: terminated (~p)", [Reason]).

handle_call({block, BatchUuid}, _From, St) ->
    {S, MS} = just_time:precise_time(),
    CS = S * 100 + MS div 10, % to centiseconds.
    ok = toke_drv:insert(St#st.toke, BatchUuid, <<CS:40>>),
    Index = index_insert(St#st.index, CS, BatchUuid),
    ?log_info("Request blocker: blocked ~p", [BatchUuid]),
    {reply, ok, cut_overhead(St#st{size = St#st.size + 1, index = Index})};

handle_call({unblock, BatchUuid}, _From, St) ->
    toke_drv:delete(St#st.toke, BatchUuid),
    ?log_info("Request blocker: unblocked ~p", [BatchUuid]),
    {reply, ok, St#st{size = St#st.size - 1}};

handle_call({is_blocked, BatchUuid}, _From, St) ->
    case toke_drv:get(St#st.toke, BatchUuid) of
        not_found -> {reply, false, St};
        _         -> {reply, true, St}
    end;

handle_call(fetch_all, _From, St) ->
    Toke = St#st.toke,
    Keys = toke_drv:fold(fun(Key, _Value, Acc) -> [Key | Acc] end, [], Toke),
    {reply, Keys, St}.

handle_cast(remove_all, St) ->
    Toke = St#st.toke,
    Keys = toke_drv:fold(fun(Key, _Value, Acc) -> [Key | Acc] end, [], Toke),
    [toke_drv:delete(Toke, Key) || Key <- Keys],
    {noreply, St};

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% ===================================================================
%% Internal
%% ===================================================================

init_state(Toke) ->
    F = fun(Uuid, <<CS:40>>, {Index, Size}) ->
            {index_insert(Index, CS, Uuid), Size + 1}
        end,
    toke_drv:fold(F, {gb_trees:empty(), 0}, Toke).

index_insert(Index, CS, Uuid) ->
    case gb_trees:lookup(CS, Index) of
        {value, Uuids} ->
            gb_trees:update(CS, [Uuid|Uuids], Index);
        none ->
            gb_trees:insert(CS, [Uuid], Index)
    end.

cut_overhead(St) ->
    case St#st.size > St#st.max of
        true ->
            {_CS, Uuids, Index} = gb_trees:take_smallest(St#st.index),
            lists:foreach(fun(Uuid) -> toke_drv:delete(St#st.toke, Uuid) end, Uuids),
            cut_overhead(St#st{index = Index, size = St#st.size - length(Uuids)});
        false ->
            St
    end.

init_cabinet(Toke, FileName) ->
    NewRes = toke_drv:new(Toke),
    SetCacheRes = toke_drv:set_cache(Toke, 1000),
    SetDfUnitRes = toke_drv:set_df_unit(Toke, 8),
    OpenRes = toke_drv:open(Toke, FileName, [read, write, create]),
    case OpenRes of
        ok -> ok;
        _Error ->
            %% tokyo file is corrupted. try to recover...
            ?log_error("Request blocker: tokyo hash table ~s open failed with results: ~p", [
                FileName, [
                    {new, NewRes},
                    {set_cache, SetCacheRes},
                    {set_df_unit, SetDfUnitRes},
                    {open, OpenRes}
                ]]),
                RootName = filename:rootname(FileName, ".tch"),
                Timestamp = "20" ++
                    just_time:unix_time_to_utc_string(just_time:unix_time()),
                BakName = lists:concat([RootName, "-", Timestamp, ".tch"]),
                ?log_error("Request blocker: renaming tokyo hash table ~s to ~s",
                    [FileName, BakName]),
                ok = file:rename(FileName, BakName),
                ok = toke_drv:open(Toke, FileName, [read, write, create])
    end.
