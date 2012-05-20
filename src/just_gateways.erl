-module(just_gateways).

-include("gateway.hrl").

-behaviour(gen_server).

%% API exports.
-export([start_link/1]).
-export([stop/0, update/0]).

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

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([GatewaySupSup]) ->
    Gateways = just_mib:gateways(),
    lager:info("Just: starting ~w gateways", [length(Gateways)]),
    [ start_gateway(GatewaySupSup, G) || G <- Gateways ],
    {ok, #st{sup = GatewaySupSup, gateways = Gateways}}.

terminate(_Reason, _St) ->
    ok.

handle_call(stop, _From, St) ->
    lager:info("Just: stopping ~w gateways", [length(St#st.gateways)]),
    [ stop_gateway(St#st.sup, G) || G <- St#st.gateways ],
    {stop, normal, ok, St};

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast(update, St) ->
    UpToDate = just_mib:gateways(),
    [ begin
          lager:info("Just: stopping removed gateway #~s#", [G#gateway.name]),
          stop_gateway(St#st.sup, G)
      end || G <- St#st.gateways -- UpToDate ],
    [ begin
          lager:info("Just: starting added gateway #~s#", [G#gateway.name]),
          start_gateway(St#st.sup, G)
      end || G <- UpToDate -- St#st.gateways ],
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

start_gateway(Sup, Gateway) ->
    just_gateway_sup_sup:start_gateway_sup(Sup, Gateway).

stop_gateway(Sup, Gateway) ->
    try just_gateway:stop(Gateway#gateway.uuid) of
        ok -> ok
    catch
        _:{noproc, _} -> ok
    end,
    just_gateway_sup_sup:terminate_gateway_sup(Sup, Gateway),
    just_gateway_sup_sup:delete_gateway_sup(Sup, Gateway).
