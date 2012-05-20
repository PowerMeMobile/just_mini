%%% TODO: use publisher confirms instead of batches in a transaction.

-module(just_incoming_sink).

-include("gateway.hrl").

-define(name(UUID, Type), {UUID, incoming_sink, Type}).
-define(pid(UUID, Type), gproc:lookup_local_name(?name(UUID, Type))).

-define(max_publish_size, 2000).    % publish no more than this many things at a time.
-define(min_publish_interval, 200). % publish no oftener than this many milliseconds.

-behaviour(gen_server).

%% API exports.
-export([start_link/3]).
-export([stop/2, notify/3]).

%% gen_server exports.
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

-type type() :: message | receipt.

-record(st, {uuid :: binary(),
             name :: string(),
             type :: type(),
             sup :: pid(), % publsher supervisor
             backlog :: queue(),
             size :: non_neg_integer(), % size of the backlog
             chan :: pid(),
             queue :: binary(),
             publishing = [] :: [binary()],
             publisher :: pid(),
             waiting = false :: boolean()}).

%% -------------------------------------------------------------------------
%% API
%% -------------------------------------------------------------------------

-spec start_link(#gateway{}, type(), pid()) -> {ok, pid()}.
start_link(Gateway, Type, PublisherSup) ->
    gen_server:start_link(?MODULE, [Gateway, Type, PublisherSup], []).

-spec stop(binary(), type()) -> ok.
stop(UUID, Type) ->
    gen_server:call(?pid(UUID, Type), stop, infinity).

%% @doc Notify the sink that a message or a receipt has been inserted into
%% the tokyo cabinet table.
-spec notify(binary(), type(), binary()) -> ok.
notify(UUID, Type, IncomingUUID) ->
    gen_server:cast(?pid(UUID, Type), {notify, IncomingUUID}).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([Gateway, Type, PublisherSup]) ->
    #gateway{uuid = UUID, name = Name} = Gateway,
    lager:info("Gateway #~s#: initializing ~s sink", [Name, Type]),
    gproc:add_local_name(?name(UUID, Type)),
    Backlog = init_state(just_cabinets:table(UUID, Type)),
    Size = queue:len(Backlog),
    lager:info("Gateway #~s#: ~w ~ss in the backlog", [Name, Size, Type]),
    Queue = case Type of
                message -> just_app:get_env(message_queue);
                receipt -> just_app:get_env(receipt_queue)
            end,
    lager:info("Gateway #~s#: ~s sink will publish to ~s", [Name, Type, Queue]),
    St = setup_chan(#st{uuid = UUID, type = Type, name = Name, sup = PublisherSup,
                        backlog = Backlog, size = Size,
                        queue = list_to_binary(Queue)}),
    lager:info("Gateway #~s#: initialized ~s sink", [Name, Type]),
    {ok, maybe_publish(St)}.

terminate(_Reason, _St) ->
    ok.

%% If a publisher hasn't finished yet, wait for its DOWN message and stop
%% in handle_info. Otherwise just stop.
handle_call(stop, _From, St) ->
    lager:info("Gateway #~s#: stopping ~s sink", [St#st.name, St#st.type]),
    St1 = close_chan(wait_for_publisher(St)),
    lager:info("Gateway #~s#: stopped ~s sink", [St#st.name, St#st.type]),
    {stop, normal, ok, St1};

handle_call(Request, _From, St) ->
    {stop, {unexpected_call, Request}, St}.

handle_cast({notify, UUID}, St) ->
    {noreply, maybe_publish(St#st{backlog = queue:in(UUID, St#st.backlog),
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
%% handle publisher exit
%% -------------------------------------------------------------------------

handle_publisher_down(normal, St) ->
    St#st{publishing = []};
%% most likely the AMQP channel death while publishing.
handle_publisher_down(Abnormal, St) ->
    lager:error("Gateway #~s#: ~s sink's publisher crashed (~w)",
                [St#st.name, St#st.type, Abnormal]),
    Backlog = lists:foldl(fun(UUID, Queue) -> queue:in_r(UUID, Queue) end,
                          St#st.backlog, St#st.publishing),
    St#st{backlog = Backlog, publishing = [],
          size = St#st.size + length(St#st.publishing)}.

%% -------------------------------------------------------------------------
%% other private functions
%% -------------------------------------------------------------------------

init_state(Toke) ->
    toke_drv:fold(fun(Key, _, Backlog) -> queue:in(Key, Backlog) end,
                  queue:new(), Toke).

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
    {Backlog, ToPublish, Size} = pick_to_publish(St#st.backlog),
    Pid = just_publisher_sup:start_publisher(St#st.sup, St#st.uuid, St#st.name,
                                             St#st.type, St#st.chan),
    monitor(process, Pid),
    just_publisher:publish(Pid, ToPublish),
    St#st{publisher = Pid, publishing = ToPublish, backlog = Backlog,
          size = St#st.size - Size, waiting = true}.

pick_to_publish(Backlog) ->
    pick_to_publish(Backlog, [], 0).

pick_to_publish(Backlog, ToPublish, Size) when Size =:= ?max_publish_size ->
    {Backlog, ToPublish, Size};
pick_to_publish(Backlog, ToPublish, Size) ->
    case queue:out(Backlog) of
        {{value, UUID}, NewBacklog} ->
            pick_to_publish(NewBacklog, [UUID|ToPublish], Size + 1);
        {empty, _} ->
            {Backlog, ToPublish, Size}
    end.
