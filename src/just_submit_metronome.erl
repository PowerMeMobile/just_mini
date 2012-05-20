-module(just_submit_metronome).

-behaviour(gen_server).

%% API exports
-export([start_link/2, stop/1, start/2, pause/1, resume/1]).

%% gen_server exports
-export([init/1, terminate/2, handle_call/3, handle_cast/2, handle_info/2,
         code_change/3]).

%% internal exports
-export([worker_loop/4]).

-define(MAX_RPS(RPS), if RPS < 25 -> RPS; true -> 25 end). % per worker.

-record(st, {queue :: pid(), rps :: non_neg_integer(), client :: pid(),
             workers = [] :: [pid()]}).

%% -------------------------------------------------------------------------
%% API functions
%% -------------------------------------------------------------------------

-spec start_link(binary(), non_neg_integer()) -> {ok, pid()}.
start_link(UUID, RPS) ->
    gen_server:start_link(?MODULE, [UUID, RPS], []).

-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:call(Pid, stop, infinity).

-spec start(pid(), pid()) -> ok.
start(Pid, Client) ->
    gen_server:cast(Pid, {start, Client}).

-spec pause(pid()) -> ok.
pause(Pid) ->
    gen_server:cast(Pid, pause).

-spec resume(pid()) -> ok.
resume(Pid) ->
    gen_server:cast(Pid, resume).

%% -------------------------------------------------------------------------
%% gen_server callback functions
%% -------------------------------------------------------------------------

init([UUID, RPS]) ->
    {ok, #st{queue = just_submit_queue:pid(UUID), rps = RPS}}.

terminate(_Reason, _St) ->
    ok.

handle_call(stop, _From, St) ->
    lists:foreach(fun worker_stop/1, St#st.workers),
    {stop, normal, ok, St}.

handle_cast({start, Client}, St) ->
    Workers = init_workers(St#st.queue, Client, St#st.rps),
    {noreply, St#st{client = Client, workers = Workers}};

handle_cast(pause, St) ->
    lists:foreach(fun worker_stop/1, St#st.workers),
    {noreply, St#st{workers = []}};

handle_cast(resume, St) ->
    Workers = init_workers(St#st.queue, St#st.client, St#st.rps),
    {noreply, St#st{workers = Workers}}.

handle_info(Info, St) ->
    {stop, {unexpected_info, Info}, St}.

code_change(_OldVsn, St, _Extra) ->
    {ok, St}.

%% -------------------------------------------------------------------------
%% init workers
%% -------------------------------------------------------------------------

init_workers(_Queue, _Client, 0) ->
    [];
init_workers(Queue, Client, RPS) ->
    Gap  = 1000 div (RPS div ?MAX_RPS(RPS)), % time between workers launching.
    % FIXME: remove this delay.
    Wait = 1000 - milliseconds(),            % time to the end of this second.
    init_workers(Queue, Client, RPS, Wait, Gap, []).

init_workers(_Queue, _Client, 0, _Wait, _Gap, Acc) ->
    Acc;
init_workers(Queue, Client, RPS, Wait, Gap, Acc) ->
    Interval = 1000 div ?MAX_RPS(RPS), % interval of individual worker.
    Slot = Gap * length(Acc),          % relative position within the second.
    Pid = worker(Queue, Client, Interval, Slot, Wait),
    init_workers(Queue, Client, RPS - ?MAX_RPS(RPS), Wait, Gap, [Pid|Acc]).

%% -------------------------------------------------------------------------
%% worker loop
%% -------------------------------------------------------------------------

worker(Queue, Client, Interval, Slot, Wait) ->
    spawn_link(fun() -> worker_init(Queue, Client, Interval, Slot, Wait) end).

worker_init(Queue, Client, Interval, Slot, Wait) ->
    timer:sleep(Wait + Slot),
    worker_loop(Queue, Client, Interval, Slot).

worker_loop(Queue, Client, Interval, Slot) ->
    erlang:send_after(Interval, self(), continue),
    case worker_work(Queue, Client) of
        sleep -> worker_sleep(Interval, Slot);
        ok    -> ok
    end,
    worker_wait(),
    ?MODULE:worker_loop(Queue, Client, Interval, Slot).

worker_work(Queue, Client) ->
    case just_submit_queue:out(Queue, wakeup) of
        {value, {Params, From}, Expiry} ->
            try just_smpp_client:submit(Client, Params, From, wakeup) of
                ok ->
                    ok;
                blocked ->
                    just_submit_queue:in_r(Queue, {Params, From}, Expiry),
                    sleep
            catch
                _:{noproc, _} ->
                    just_submit_queue:in_r(Queue, {Params, From}, Expiry),
                    exit(normal)
            end;
        empty ->
            sleep
    end.

%% wait for 'continue' atom.
worker_wait() ->
    receive
        continue -> ok;
        stop     -> exit(normal)
    end.

%% wait to be woken up (new message in the queue or an unthrottled session).
worker_sleep(Interval, Slot) ->
    receive
        wakeup ->
            Ms = milliseconds(),
            if
                Ms < Slot            -> timer:sleep(Slot - Ms);
                Ms < Slot + Interval -> ok;
                true                 -> timer:sleep(1000 - Ms + Slot)
            end;
        stop ->
            exit(normal)
    end.

worker_stop(Pid) ->
    Pid ! stop.

%% -------------------------------------------------------------------------
%% other private functions
%% -------------------------------------------------------------------------

milliseconds() ->
    {_, _, MicroSecs} = os:timestamp(),
    MicroSecs div 1000.
