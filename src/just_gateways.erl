-module(just_gateways).

-include("gateway.hrl").
-include_lib("alley_common/include/logging.hrl").

-behaviour(gen_server).

%% API exports.
-export([start_link/1]).
-export([stop/0, update/0]).

%% Support API.
-export([list_gateways/0, start_gateway/1, stop_gateway/1]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-record(st, {sup :: pid(), gateways :: [#gateway{}]}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(pid()) -> {ok, pid()}.
start_link(GatewaySupSup) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [GatewaySupSup], []).

%% @doc Gracefully stops every running gateway and itself.
%% Should only be called once, immediately prior to the application shutdown.
-spec stop() -> ok.
stop() ->
    gen_server:call(?MODULE, stop, infinity).

-spec update() -> ok.
update() ->
    gen_server:cast(?MODULE, update).

-spec list_gateways() -> [#gateway{}].
list_gateways() ->
    gen_server:call(?MODULE, list_gateways, 50000).

-spec start_gateway(binary()) ->
    ok | {error, already_started} | {error, not_found}.
start_gateway(Uuid) ->
    Gateways = just_mib:gateways(),
    case lists:keyfind(Uuid, #gateway.uuid, Gateways) of
        false ->
            {error, not_found};
        G ->
            gen_server:call(?MODULE, {start_gateway, G}, 10000)
    end.

-spec stop_gateway(binary()) ->
    ok | {error, not_started} | {error, not_found}.
stop_gateway(Uuid) ->
    Gateways = just_mib:gateways(),
    case lists:keyfind(Uuid, #gateway.uuid, Gateways) of
        false ->
            {error, not_found};
        G ->
            gen_server:call(?MODULE, {stop_gateway, G}, 10000)
    end.

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([GatewaySupSup]) ->
    Gateways = just_mib:gateways(),
    ?log_info("Just: starting ~w gateways", [length(Gateways)]),
    [start_gateway(GatewaySupSup, G) || G <- Gateways],
    {ok, #st{sup = GatewaySupSup, gateways = Gateways}}.

terminate(_Reason, _St) ->
    ok.

handle_call(stop, _From, St) ->
    ?log_info("Just: stopping ~w gateways", [length(St#st.gateways)]),
    [stop_gateway(St#st.sup, G) || G <- St#st.gateways],
    {stop, normal, ok, St};

handle_call(list_gateways, _From, St) ->
    {reply, St#st.gateways, St};

handle_call({start_gateway, G}, _From, St) ->
    Gateways = St#st.gateways,
    case lists:keyfind(G#gateway.uuid, #gateway.uuid, Gateways) of
        false ->
            {ok, _Pid} = start_gateway(St#st.sup, G),
            {reply, ok, St#st{gateways = Gateways ++ [G]}};
        _Gtw ->
            {reply, {error, already_started}, St}
    end;

handle_call({stop_gateway, G}, _From, St) ->
    Gateways = St#st.gateways,
    case lists:keyfind(G#gateway.uuid, #gateway.uuid, Gateways) of
        false ->
            {reply, {error, not_started}, St};
        _Gtw ->
            ok = stop_gateway(St#st.sup, G),
            {reply, ok, St#st{gateways = Gateways -- [G]}}
    end;

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast(update, St) ->
    UpToDate = just_mib:gateways(),
    [stop_gateway(St#st.sup, G) || G <- St#st.gateways -- UpToDate],
    [start_gateway(St#st.sup, G) || G <- UpToDate -- St#st.gateways],
    {noreply, St#st{gateways = UpToDate}};

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% private functions
%% -------------------------------------------------------------------------

start_gateway(Sup, G) ->
    ?log_info("Just: starting added gateway #~s#", [G#gateway.name]),
    just_gateway_sup_sup:start_gateway_sup(Sup, G).

stop_gateway(Sup, G) ->
    ?log_info("Just: stopping removed gateway #~s#", [G#gateway.name]),
    try just_gateway:stop(G#gateway.uuid) of
        ok -> ok
    catch
        _:{noproc, _} -> ok
    end,
    just_gateway_sup_sup:terminate_gateway_sup(Sup, G),
    just_gateway_sup_sup:delete_gateway_sup(Sup, G).
