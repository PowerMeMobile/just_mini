%%% TODO: use publisher confirms instead of batches in a transaction.

-module(just_response_sink).

-include("gateway.hrl").
-include("persistence.hrl").

-define(name(UUID), {UUID, response_sink}).
-define(pid(UUID), gproc:lookup_local_name(?name(UUID))).

-define(max_publish_size, 2000).    % publish no more than this many responses at a time.
-define(min_publish_interval, 200). % publish no oftener than this many milliseconds.

-behaviour(gen_server).

%% API exports.
-export([start_link/2]).
-export([stop/1, notify/4]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-record(st, {uuid :: binary(),
             name :: string(),
             sup :: pid(), % publisher supervisor
             backlog :: dict(),
             size :: non_neg_integer(), % size of the backlog
             index :: gb_tree(), % backlog index by accepted_at
             chan :: pid(),
             queue :: binary(),
             publishing = [] :: [{binary(), [binary()], just_calendar:precise_time()}],
             publisher :: pid(),
             waiting = false :: boolean()}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}, pid()) -> {ok, pid()}.
start_link(Gateway, PublisherSup) ->
    gen_server:start_link(?MODULE, [Gateway, PublisherSup], []).

-spec stop(binary()) -> ok.
stop(UUID) ->
    gen_server:call(?pid(UUID), stop, infinity).

%% @doc Notify the sink that a response has been inserted into the tokyo
%% cabinet table.
-spec notify(binary(), binary(), binary(), just_calendar:precise_time()) -> ok.
notify(UUID, BatchUUID, SegmentUUID, AcceptedAt) ->
    gen_server:cast(?pid(UUID), {notify, BatchUUID, SegmentUUID, AcceptedAt}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([Gateway, PublisherSup]) ->
    #gateway{uuid = UUID, name = Name} = Gateway,
    lager:info("Gateway #~s#: initializing response sink", [Name]),
    gproc:add_local_name(?name(UUID)),
    {{Backlog, Index}, Size} = init_state(just_cabinets:table(UUID, response)),
    lager:info("Gateway #~s#: ~w responses in the backlog", [Name, Size]),
    Queue = just_app:get_env(response_queue),
    lager:info("Gateway #~s#: response sink will publish to ~s", [Name, Queue]),
    St = setup_chan(#st{uuid = UUID, name = Name, sup = PublisherSup,
                        backlog = Backlog, index = Index, size = Size,
                        queue = list_to_binary(Queue)}),
    lager:info("Gateway #~s#: initialized response sink", [Name]),
    {ok, maybe_publish(St)}.

terminate(_Reason, _St) ->
    ok.

%% If a publisher hasn't finished yet, wait for its DOWN message and stop
%% in handle_info. Otherwise just stop.
handle_call(stop, _From, St) ->
    lager:info("Gateway #~s#: stopping response sink", [St#st.name]),
    St1 = close_chan(wait_for_publisher(St)),
    lager:info("Gateway #~s#: stopped response sink", [St#st.name]),
    {stop, normal, ok, St1};

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast({notify, BatchUUID, SegmentUUID, AcceptedAt}, St) ->
    {Backlog, Index} = backlog_update(BatchUUID, SegmentUUID, AcceptedAt,
                                      St#st.backlog, St#st.index),
    {noreply, maybe_publish(St#st{backlog = Backlog, index = Index,
                                  size = St#st.size + 1})};

handle_cast(Request, St) ->
    {stop, {unexpected_cast, Request}, St}.

handle_info({'DOWN', _MRef, process, Pid, Reason}, #st{publisher = Pid} = St) ->
    {noreply, maybe_publish(handle_publisher_down(Reason,
                                                  St#st{publisher = undefined}))};

handle_info({'DOWN', _MRef, process, Pid, _Info}, #st{chan = Pid} = St) ->
    {noreply, maybe_publish(setup_chan(St#st{chan = undefined}))};

handle_info(amqp_conn_ready, St) ->
    {noreply, maybe_publish(setup_chan(St))};

handle_info({timeout, _TRef, waiting}, St) ->
    {noreply, maybe_publish(St#st{waiting = false})};

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% handle publisher down
%% -------------------------------------------------------------------------

handle_publisher_down(normal, St) ->
    St#st{publishing = []};
%% most likely the AMQP channel death while publishing.
handle_publisher_down(Abnormal, St) ->
    lager:error("Gateway #~s#: response sink's publisher crashed (~w)",
                [St#st.name, Abnormal]),
    % merge {Batch, Segments, AcceptedAt} list from St#st.publishing with current
    % versions of Backlog and Index and then try publishing again.
    {B, I} =
        lists:foldl(fun({Batch, Segments, AcceptedAt}, {Backlog, Index}) ->
                        backlog_update(Batch, Segments, AcceptedAt, Backlog, Index)
                    end,
                    {St#st.backlog, St#st.index}, St#st.publishing),
    Size = lists:sum([ length(Segments) || {_, Segments, _} <- St#st.publishing ]),
    St#st{backlog = B, index = I, size = St#st.size + Size, publishing = []}.

%% -------------------------------------------------------------------------
%% other private functions
%% -------------------------------------------------------------------------

init_state(Toke) ->
    % Key is the batch segment UUID.
    % Batch is the UUID of the original batch.
    F = fun(Key, Resp, {{Backlog, Index}, Size}) ->
            #response{batch = Batch, accepted_at = AcceptedAt} = binary_to_term(Resp),
            {backlog_update(Batch, Key, AcceptedAt, Backlog, Index), Size + 1}
        end,
    toke_drv:fold(F, {{dict:new(), gb_trees:empty()}, 0}, Toke).

backlog_update(Batch, SegmentOrSegments, AcceptedAt, Backlog, Index) ->
    Segments = case is_list(SegmentOrSegments) of
                   true  -> SegmentOrSegments;
                   false -> [SegmentOrSegments]
               end,
    case dict:is_key(Batch, Backlog) of
        true ->
            {dict:update(Batch, fun(Old) -> Segments ++ Old end, Backlog), Index};
        false ->
            {dict:store(Batch, Segments, Backlog),
             case gb_trees:lookup(AcceptedAt, Index) of
                 {value, Batches} ->
                     gb_trees:update(AcceptedAt, [Batch|Batches], Index);
                 none ->
                     gb_trees:insert(AcceptedAt, [Batch], Index)
             end}
    end.

setup_chan(St) ->
    case just_amqp_conn:open_channel(St#st.uuid) of
        {ok, Chan} ->
            monitor(process, Chan),
            just_amqp:tx_select(Chan),
            ok = just_amqp:declare_queue(Chan, St#st.queue),
            St#st{chan = Chan};
        unavailable ->
            St
    end.

wait_for_publisher(St) ->
    case St#st.publisher of
        undefined ->
            St;
        Pid ->
            receive
                {'DOWN', _MRef, process, Pid, Reason} ->
                    handle_publisher_down(Reason, St)
            end
    end.

close_chan(St) ->
    case St#st.chan of
        undefined ->
            ok;
        Chan ->
            amqp_channel:close(Chan),
            St#st{chan = undefined}
    end.

maybe_publish(#st{publisher = Pid} = St) when is_pid(Pid) ->
    % current publisher hasn't finished yet.
    St;
maybe_publish(#st{chan = undefined} = St) ->
    % AMQP channel is still down.
    St;
maybe_publish(#st{waiting = true} = St) ->
    % still waiting for the timer message to arrive.
    St;
maybe_publish(#st{size = 0} = St) ->
    % nothing to publish yet.
    St;
maybe_publish(St) ->
    erlang:start_timer(?min_publish_interval, self(), waiting),
    {Backlog, Index, ToPublish, Size} = pick_to_publish(St#st.backlog, St#st.index),
    Pid = just_publisher_sup:start_publisher(St#st.sup, St#st.uuid, St#st.name,
                                             response, St#st.chan),
    monitor(process, Pid),
    just_publisher:publish(Pid, ToPublish),
    St#st{publisher = Pid, publishing = ToPublish, backlog = Backlog,
          index = Index, size = St#st.size - Size, waiting = true}.

pick_to_publish(Backlog, Index) ->
    pick_to_publish(Backlog, Index, [], 0).

pick_to_publish(Backlog, Index, ToPublish, Size) when Size >= ?max_publish_size ->
    {Backlog, Index, ToPublish, Size};
pick_to_publish(Backlog, Index, ToPublish, Size) ->
    {AcceptedAt, Batches, Index1} = gb_trees:take_smallest(Index),
    {Backlog1, ToPublish1, LeftOver, Size1} =
        pick_to_publish2(AcceptedAt, Backlog, Batches, ToPublish, Size),
    Index2 = case LeftOver of
                 [] -> Index1;
                 _  -> gb_trees:insert(AcceptedAt, LeftOver, Index1)
             end,
    case gb_trees:is_empty(Index2) of
        true ->
            {Backlog1, Index2, ToPublish1, Size1};
        false ->
            pick_to_publish(Backlog1, Index2, ToPublish1, Size1)
    end.

pick_to_publish2(_AcceptedAt, Backlog, LeftOver, ToPublish, Size)
        when Size >= ?max_publish_size ->
    {Backlog, ToPublish, LeftOver, Size};
pick_to_publish2(_AcceptedAt, Backlog, [], ToPublish, Size) ->
    {Backlog, ToPublish, [], Size};
pick_to_publish2(AcceptedAt, Backlog, [Batch|Tail], ToPublish, Size) ->
    Segments = dict:fetch(Batch, Backlog),
    pick_to_publish2(AcceptedAt, dict:erase(Batch, Backlog), Tail,
                     [{Batch, Segments, AcceptedAt}|ToPublish],
                     Size + length(Segments)).
