-module(just_dedup).

-behaviour(gen_server).

%% API exports.
-export([start_link/1]).
-export([is_dup/2, insert/2]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-record(st, {uuid :: binary(), toke :: pid(), index :: gb_tree(),
             size :: non_neg_integer(), max :: non_neg_integer()}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(binary()) -> {ok, pid()}.
start_link(UUID) ->
    gen_server:start_link(?MODULE, [UUID], []).

-spec is_dup(pid(), binary()) -> boolean().
is_dup(Pid, UUID) ->
    gen_server:call(Pid, {is_dup, UUID}, infinity).

-spec insert(pid(), binary()) -> ok.
insert(Pid, UUID) ->
    gen_server:call(Pid, {insert, UUID}, infinity).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([UUID]) ->
    Toke = just_cabinets:table(UUID, dedup),
    {Index, Size} = init_state(Toke),
    {ok, cut_overhead(#st{uuid = UUID, toke = Toke, index = Index, size = Size,
                          max = 100000})}.

terminate(_Reason, _St) ->
    ok.

handle_call({is_dup, UUID}, _From, St) ->
    {reply, toke_drv:get(St#st.toke, UUID) =/= not_found, St};

handle_call({insert, UUID}, _From, St) ->
    {S, MS} = just_time:precise_time(),
    CS = S * 100 + MS div 10, % to centiseconds.
    toke_drv:insert_async(St#st.toke, UUID, <<CS:40>>),
    Index = index_insert(St#st.index, CS, UUID),
    {reply, ok, cut_overhead(St#st{size = St#st.size + 1, index = Index})};

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% private functions
%% -------------------------------------------------------------------------

init_state(Toke) ->
    F = fun(UUID, <<CS:40>>, {Index, Size}) ->
            {index_insert(Index, CS, UUID), Size + 1}
        end,
    toke_drv:fold(F, {gb_trees:empty(), 0}, Toke).

index_insert(Index, CS, UUID) ->
    case gb_trees:lookup(CS, Index) of
        {value, UUIDs} ->
            gb_trees:update(CS, [UUID|UUIDs], Index);
        none ->
            gb_trees:insert(CS, [UUID], Index)
    end.

cut_overhead(St) ->
    case St#st.size > St#st.max of
        true ->
            {_CS, UUIDs, Index} = gb_trees:take_smallest(St#st.index),
            lists:foreach(fun(UUID) -> toke_drv:delete(St#st.toke, UUID) end, UUIDs),
            cut_overhead(St#st{index = Index, size = St#st.size - length(UUIDs)});
        false ->
            St
    end.
