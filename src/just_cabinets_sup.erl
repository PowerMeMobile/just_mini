-module(just_cabinets_sup).

-behaviour(supervisor).

-export([start_link/1]).
-export([init/1]).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(binary()) -> {ok, pid(), pid()}.
start_link(UUID) ->
    {ok, Sup} = supervisor:start_link(?MODULE, []),
    {ok, Dedup} = start_toke(Sup, dedup),
    {ok, Request} = start_toke(Sup, request),
    {ok, Response} = start_toke(Sup, response),
    {ok, Delivery} = start_toke(Sup, delivery),
    {ok, Receipt} = start_toke(Sup, receipt),
    {ok, Cabinets} =
        supervisor:start_child(Sup,
            {cabinets, {just_cabinets, start_link,
                        [UUID, [Dedup, Request, Response, Delivery, Receipt]]},
             transient, 5000, worker, [just_cabinets]}),
    {ok, Sup, Cabinets}.

%% -------------------------------------------------------------------------
%% supervisor callback functions
%% -------------------------------------------------------------------------

init([]) ->
    {ok, {{one_for_all, 0, 1}, []}}.

%% -------------------------------------------------------------------------
%% private functions
%% -------------------------------------------------------------------------

start_toke(Sup, Table) ->
    supervisor:start_child(Sup,
        {Table, {toke_drv, start_link, []}, transient, 5000, worker, [toke_drv]}).
