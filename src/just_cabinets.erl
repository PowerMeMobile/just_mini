-module(just_cabinets).

-include("gateway.hrl").
-include_lib("alley_common/include/logging.hrl").

-define(name(UUID), {UUID, cabinets}).
-define(pid(UUID), gproc:lookup_local_name(?name(UUID))).

-define(names, [dedup, request, response, incoming, receipt]).

-behaviour(gen_server).

%% API exports.
-export([start_link/2]).
-export([stop/1, table/2]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-record(st, {uuid :: binary(), name :: string(), tables :: [pid()]}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}, [pid()]) -> {ok, pid()}.
start_link(Gateway, Tables) ->
    gen_server:start_link(?MODULE, [Gateway, Tables], []).

-spec stop(binary()) -> ok.
stop(UUID) ->
    gen_server:call(?pid(UUID), stop, infinity).

-spec table(binary(), dedup | request | response | incoming | receipt) -> pid().
table(UUID, Name) ->
    gen_server:call(?pid(UUID), {table, Name}, infinity).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([Gateway, Tables]) ->
    #gateway{uuid = UUID, name = Name} = Gateway,
    ?log_info("Gateway #~s#: initializing tokyo hash tables", [Name]),
    gproc:add_local_name(?name(UUID)),
    Dir = filename:join(["data", "gateways",
                         binary_to_list(uuid:unparse_lower(UUID))]),
    file:make_dir(Dir),
    lists:foreach(fun({N, T}) ->
                      FileName = filename:join(Dir, lists:concat([N, ".tch"])),
                      ok = init_cabinet(T, FileName, Name)
                  end, lists:zip(?names, Tables)),
    ?log_info("Gateway #~s#: initialized tokyo hash tables", [Name]),
    {ok, #st{uuid = UUID, name = Name, tables = Tables}}.

terminate(_Reason, _St) ->
    ok.

handle_call(stop, _From, St) ->
    ?log_info("Gateway #~s#: stopping tokyo hash tables", [St#st.name]),
    lists:foreach(fun(T) ->
                      toke_drv:close(T),
                      toke_drv:delete(T),
                      toke_drv:stop(T)
                  end, St#st.tables),
    ?log_info("Gateway #~s#: stopped tokyo hash tables", [St#st.name]),
    {stop, normal, ok, St};

handle_call({table, Name}, _From, St) ->
    {reply, proplists:get_value(Name, lists:zip(?names, St#st.tables)), St};

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% Internal
%% -------------------------------------------------------------------------

init_cabinet(Toke, FileName, GatewayName) ->
    NewRes = toke_drv:new(Toke),
    SetCacheRes = toke_drv:set_cache(Toke, 100000),
    SetDfUnitRes = toke_drv:set_df_unit(Toke, 8),
    TuneRes = toke_drv:tune(Toke, 0, 8, 10, [large]),
    OpenRes = toke_drv:open(Toke, FileName, [read, write, create]),
    case OpenRes of
        ok -> ok;
        _Error ->
            %% tokyo file is corrupted. try to recover...
            ?log_error("Gateway #~s#: tokyo hash table ~s open failed with results: ~p", [
                GatewayName, FileName, [
                    {new, NewRes},
                    {set_cache, SetCacheRes},
                    {set_df_unit, SetDfUnitRes},
                    {tune, TuneRes},
                    {open, OpenRes}
                ]]),
                RootName = filename:rootname(FileName, ".tch"),
                Timestamp = "20" ++
                    just_time:unix_time_to_utc_string(just_time:unix_time()),
                BakName = lists:concat([RootName, "-", Timestamp, ".tch"]),
                ?log_error("Gateway #~s#: renaming tokyo hash table ~s to ~s",
                    [GatewayName, FileName, BakName]),
                ok = file:rename(FileName, BakName),
                ok = toke_drv:open(Toke, FileName, [read, write, create])
    end.
