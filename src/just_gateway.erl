-module(just_gateway).

-include("gateway.hrl").
-include_lib("alley_common/include/logging.hrl").

-define(name(UUID), {UUID, gateway}).
-define(pid(UUID), gproc:lookup_local_name(?name(UUID))).

-behaviour(gen_server).

%% API exports.
-export([start_link/1]).
-export([stop/1]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-record(st, {uuid :: binary(), name :: string()}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}) -> {ok, pid()}.
start_link(Gateway) ->
    gen_server:start_link(?MODULE, [Gateway], []).

-spec stop(binary()) -> ok.
stop(UUID) ->
    gen_server:call(?pid(UUID), stop, infinity).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([Gateway]) ->
    #gateway{uuid = UUID, name = Name} = Gateway,
    gproc:add_local_name(?name(UUID)),
    ?log_info("Gateway #~s#: initialized", [Name]),
    {ok, #st{uuid = UUID, name = Name}}.

terminate(_Reason, _St) ->
    ok.

handle_call(stop, _From, St) ->
    ?log_info("Gateway #~s#: stopping", [St#st.name]),
    UUID = St#st.uuid,
    just_request_acceptor:stop(UUID),
    just_scheduler:stop(UUID),
    just_smpp_clients:stop(UUID),
    just_submit_queue:stop(UUID),
    just_sink:stop(UUID, incoming),
    just_sink:stop(UUID, receipt),
    just_sink:stop(UUID, response),
    just_amqp_conn:stop(UUID),
    just_cabinets:stop(UUID),
    ?log_info("Gateway #~s#: stopped", [St#st.name]),
    {stop, normal, ok, St};

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.
