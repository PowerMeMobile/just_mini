-module(just_customers).

-include("customer.hrl").

-behaviour(gen_server).

-define(customer_ets, just_customers_ets).

%% API exports.
-export([start_link/1]).
-export([request/3, update/1, subscribe/0]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-record(st, {sup :: pid(), subscribers = []}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(pid()) -> {ok, pid()}.
start_link(CustomerSup) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [CustomerSup], []).

-spec request(binary(), pos_integer(), just_calendar:precise_time()) ->
              true | false | undefined.
request(UUID, Reqs, Time) ->
    case ets:lookup(?customer_ets, UUID) of
        [{UUID, Pid}] ->
            just_customer:request(Pid, Reqs, Time);
        [] ->
            case gen_server:call(?MODULE, {customer_pid, UUID}, infinity) of
                undefined ->
                    undefined;
                Pid ->
                    just_customer:request(Pid, Reqs, Time)
            end
    end.

%% FIXME: use gen_event.
-spec update(binary()) -> ok.
update(UUID) ->
    gen_server:cast(?MODULE, {update, UUID}).

%% FIXME: use gen_event.
%% @doc Subscribe to customer updates. Called by gateway schedulers.
-spec subscribe() -> ok.
subscribe() ->
    gen_server:call(?MODULE, subscribe, infinity).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([CustomerSup]) ->
    ?customer_ets = ets:new(?customer_ets, [named_table]),
    {ok, #st{sup = CustomerSup}}.

terminate(_Reason, _St) ->
    ok.

handle_call({customer_pid, UUID}, _From, St) ->
    case ets:lookup(?customer_ets, UUID) of
        [{UUID, Pid}] ->
            {reply, Pid, St};
        [] ->
            case just_mib:customer(UUID) of
                undefined ->
                    {reply, undefined, St};
                #customer{rps = RPS} ->
                    Pid = just_customer_sup:start_customer(St#st.sup, UUID, RPS),
                    ets:insert(?customer_ets, {UUID, Pid}),
                    _MRef = monitor(process, Pid),
                    {reply, Pid, St}
            end
    end;

handle_call(subscribe, {Pid, _Tag}, St) ->
    monitor(process, Pid),
    {reply, ok, St#st{subscribers = [Pid|St#st.subscribers]}};

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast({update, UUID}, St) ->
    [ Pid ! {customer_updated, UUID} || Pid <- St#st.subscribers ],
    case just_mib:customer(UUID) of
        undefined ->
            % the customer has been removed.
            case ets:lookup(?customer_ets, UUID) of
                [{UUID, Pid}] ->
                    ets:delete(?customer_ets, UUID),
                    just_customer:stop(Pid);
                [] ->
                    ok
            end;
        #customer{rps = RPS} ->
            % the customer has been updated or created.
            case ets:lookup(?customer_ets, UUID) of
                [{UUID, Pid}] -> just_customer:update(Pid, RPS);
                []            -> ok
            end
    end,
    {noreply, St};

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info({'DOWN', _MRef, process, Pid, Info}, St) ->
    case lists:member(Pid, St#st.subscribers) of
        true ->
            {noreply, St#st{subscribers = lists:delete(Pid, St#st.subscribers)}};
        false ->
            % a just_customer process (stopped normally by this process or else).
            case Info of
                normal ->
                    ets:match_delete(?customer_ets, {'_', Pid}),
                    {noreply, St};
                _ ->
                    {stop, {customer_down, Info}, St}
            end
    end;

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.
