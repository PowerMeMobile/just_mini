-module(just_cabinets).

-include("gateway.hrl").

-define(name(UUID), {UUID, cabinets}).
-define(pid(UUID), gproc:lookup_local_name(?name(UUID))).

-define(names, [dedup, request, response, message, receipt]).

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

-spec table(binary(), dedup | request | response | message | receipt) -> pid().
table(UUID, Name) ->
    gen_server:call(?pid(UUID), {table, Name}, infinity).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([Gateway, Tables]) ->
    #gateway{uuid = UUID, name = Name} = Gateway,
    lager:info("Gateway #~s#: initializing tokyo hash tables", [Name]),
    gproc:add_local_name(?name(UUID)),
    Dir = filename:join(["data", "gateways",
                         binary_to_list(uuid:unparse_lower(UUID))]),
    file:make_dir(Dir),
    lists:foreach(fun({N, T}) ->
                      toke_drv:new(T),
                      toke_drv:set_cache(T, 100000),
                      toke_drv:tune(T, 0, 8, 10, [large]),
                      FileName = filename:join(Dir, lists:concat([N, ".tch"])),
                      ok = toke_drv:open(T, FileName, [read, write, create])
                  end, lists:zip(?names, Tables)),
    lager:info("Gateway #~s#: initialized tokyo hash tables", [Name]),
    {ok, #st{uuid = UUID, name = Name, tables = Tables}}.

terminate(_Reason, _St) ->
    ok.

handle_call(stop, _From, St) ->
    lager:info("Gateway #~s#: stopping tokyo hash tables", [St#st.name]),
    lists:foreach(fun(T) ->
                      toke_drv:close(T),
                      toke_drv:delete(T),
                      toke_drv:stop(T)
                  end, St#st.tables),
    lager:info("Gateway #~s#: stopped tokyo hash tables", [St#st.name]),
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
