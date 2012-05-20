-module(just_smpp_client_sup).

-include("gateway.hrl").

-behaviour(supervisor2).

-export([start_link/5]).
-export([start_esme_session/2]).
-export([init/1]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(binary(), string(), pos_integer(), just_settings:settings(),
                 #smpp_connection{}) -> {ok, pid(), pid()}.
start_link(UUID, Name, RPS, Settings, Conn) ->
    {ok, Sup} = supervisor2:start_link(?MODULE, []),
    {ok, LogMgr} =
        supervisor2:start_child(Sup,
            {log_mgr, {smpp_log_mgr, start_link, []},
             permanent, 5000, worker, dynamic}),
    {ok, Metronome} =
        supervisor2:start_child(Sup,
            {metronome,
             {just_submit_metronome, start_link, [UUID, RPS]},
             transient, 5000, worker, [just_submit_metronome]}),
    {ok, Client} =
        supervisor2:start_child(Sup,
            {smpp_client,
             {just_smpp_client, start_link,
              [UUID, Name, RPS, Settings, Conn, Sup, Metronome, LogMgr]},
             intrinsic, 5000, worker, [just_smpp_client]}),
    {ok, Sup, Client}.

-spec start_esme_session(pid(), list()) -> {ok, pid()} | {error, any()}.
start_esme_session(Sup, Args) ->
    % terminate first to be sure.
    supervisor2:terminate_child(Sup, esme_session),
    supervisor2:start_child(Sup,
        {esme_session, {gen_esme_session, start_link, [just_smpp_client, Args]},
         temporary, 5000, worker, [gen_esme_session]}).

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.
