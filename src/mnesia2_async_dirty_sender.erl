%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2016. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%
%% %CopyrightEnd%
%%

%%
-module(mnesia2_async_dirty_sender).

-export([start_link/1,
         pool_name/1]).

-export([init_buffer_sender/4,
         async_dirty_sender_loop/4,
         init_async_dirty_tm_sender_loop/3]).

-import(mnesia2_lib, [verbose/2]).

-define(NUM_ASYNC_DIRTY_TM_SENDER, 3).

-record(buffer_log, {fn,
                     fnum=0,
                     fsize=0,
                     wf,
                     wtxns=[],
                     wtxnsize=0,
                     newtxns=[]}).
-record(buffer_log_send, {parent,
                          node,
                          num,
                          logname,
                          fnum,
                          fn,
                          rf,
                          filepos,
                          rtxns=[],
                          ntxns=0,
                          bytes=0,
                          runmode,
                          starttime}).

-include("mnesia2.hrl").

start_link([Node, Num, Parent]) ->
    Pid = spawn_link(?MODULE, init_async_dirty_tm_sender_loop,
                       [Node, Num, Parent]),
    {ok, Pid}.

pool_name(Node) ->
    list_to_atom("mnesia2_tm_async_dirty_sender_pool@" ++
                 atom_to_list(Node)).

async_dirty_send(_Node, []) ->
    ok;
async_dirty_send(Node, [{TmName, Txn} | Tail]) ->
    erlang:send({TmName, Node}, Txn),
    async_dirty_send(Node, Tail).

init_async_dirty_tm_sender_loop(Node, Num, Parent) ->
    case check_remote_tm(Node) of
        ok ->
            async_dirty_sender_loop(Node, Num, Parent, []);
        _ ->
            timer:sleep(60*1000),
            init_async_dirty_tm_sender_loop(Node, Num, Parent)
    end.

init_buffer_sender(Parent, Node, Num, LogName) ->
    open_async_dirty_buffer_log(#buffer_log_send{parent=Parent, node=Node, num=Num,
                                                 logname=LogName, fnum=0, runmode=running,
                                                 starttime=os:timestamp()}).

async_dirty_sender_loop(Node, Num, Parent, Mode) ->
    Timeout = case Mode of
          [] ->
              % normal mode: wait for txns to forward (non-blocking sends)
              infinity;
          #buffer_log{wtxns=[], newtxns=[]} ->
              % buffered mode: wait for txns to write to buffer log
              infinity;
          #buffer_log{} ->
              % buffered mode: have txns to write to buffer log
              0;
          force ->
              % forced mode: wait for txns to forward (blocking sends)
              infinity;
          _ ->
              % queued mode: retry sending queued txns after a brief wait
              100
          end,
    {Txns, Msg} = async_dirty_dequeue(Timeout, []),
    verbose("dequeued txns: ~p, msg: ~p, mode: ~p",
        [Txns, Msg, Mode]),
    TxBacklogThreshold = mnesia2_lib:val(async_dirty_tx_backlog_threshold),
    Mode1 = case Mode of
        Buffered when is_list(Buffered) ->
            RemTxns = case send_buffer_log_try(Node, Buffered) of
                  [] ->
                      % sent all the queued txns; try the ones we just dequeued
                      send_buffer_log_try(Node, Txns);
                  Rem ->
                      % add newly dequeued txns to remaining queue
                      lists:append(Rem, Txns)
                  end,
            if length(RemTxns) < TxBacklogThreshold ->
                   % continue to buffer in-memory
                   RemTxns;
               true ->
                   % need to start buffering on disk
                   LogName = atom_to_list(node_num_to_async_dirty_tm_sender_log(Node, Num)),
                   LogFile = lists:flatten(io_lib:format("~s.~4.10.0b", [LogName, 0])),
               case prim_file:open(mnesia2_lib:dir(LogFile),
                                   [raw, binary, append]) of
                   {ok, WF} ->
                       prim_file:truncate(WF),
                       BufferMode = append_buffer_log(#buffer_log{fn=LogName, wf=WF}, RemTxns),
                       NewPid = spawn_link(?MODULE, init_buffer_sender,
                                           [self(), Node, Num, LogName]),
                       verbose("~s: START async_dirty_sender buffer log (reader=~p)",
                            [LogName, NewPid]),
                       verbose("~s: OPEN-WRITE async_dirty_sender buffer log (seq=~b)",
                            [LogName, 0]),
                       BufferMode;
                   WError ->
                       verbose("Failure opening async_dirty_sender buffer log: ~s: ~1000p",
                            [LogFile, WError]),
                       async_dirty_send(Node, RemTxns),
                       force
               end
            end;
        #buffer_log{wtxns=undefined, newtxns=NewTxns} ->
            % sender is stopping, so we need to queue until it finishes so we don't overlap our sends
            Mode#buffer_log{newtxns=lists:append(NewTxns, Txns)};
        #buffer_log{} ->
            % write new txns to buffer log
            append_buffer_log(Mode, Txns);
        force ->
            % blocking send because buffering failed
            async_dirty_send(Node, Txns),
            force
        end,
    Mode2 = case Msg of
        undefined ->
            % no pending message
            Mode1;
        {mnesia2_down, Node} ->
            % our peer is going down
            unlink(Parent),
            exit(shutdown);
        {buffer_drained, FNum, SenderPid} when
                    length(Mode1#buffer_log.wtxns) +
                    length(Mode1#buffer_log.newtxns) > TxBacklogThreshold ->
            % the sender reached the drain threshold but we still have too much queued here;
            % continue spooling
            verbose("~s: DEFER-STOP async_dirty_sender buffer log (seq=~b wtxns=~b newtxns=~b)",
                [Mode1#buffer_log.fn, FNum, length(Mode1#buffer_log.wtxns), length(Mode1#buffer_log.newtxns)]),
            SenderPid ! {buffer_continue, Mode1#buffer_log.fnum, self()},
            Mode1;
        {buffer_drained, FNum, SenderPid} when FNum =:= Mode1#buffer_log.fnum ->
            % the sender reached the drain threshold and is stopping
            verbose("~s: DRAINED-STOP async_dirty_sender buffer log (seq=~b wtxns=~b newtxns=~b)",
                [Mode1#buffer_log.fn, FNum,
                 length(Mode1#buffer_log.wtxns), length(Mode1#buffer_log.newtxns)]),
            SenderPid ! {buffer_drained_ack, self()},
            % queue up the txns that were being buffered for a log write and append the queued txns
            Mode1#buffer_log{wtxns=undefined,
                             newtxns=lists:append(lists:reverse(Mode1#buffer_log.wtxns),
                             Mode1#buffer_log.newtxns)};
        {buffer_drained, FNum, SenderPid} ->
            verbose("~s: NEXT async_dirty_sender buffer log (read-seq=~b write-seq=~b pending=~b)",
                [Mode1#buffer_log.fn, FNum, Mode1#buffer_log.fnum, Mode1#buffer_log.fnum - FNum]),
            SenderPid ! {buffer_continue, Mode1#buffer_log.fnum, self()},
            Mode1;
        {buffer_drained_ack, _Pid} ->
            % sender has stopped so we can send our queued transactions and resume in-memory buffering
            verbose("~s: STOPPED async_dirty_sender buffer log (newtxns=~b)",
                [Mode1#buffer_log.fn, length(Mode#buffer_log.newtxns)]),
            prim_file:close(Mode#buffer_log.wf),
            Mode1#buffer_log.newtxns;
        Other ->
            verbose("mnesia2_tm async_dirty sender unknown message: ~1000p~n", [Other]),
            Mode1
        end,
    ?MODULE:async_dirty_sender_loop(Node, Num, Parent, Mode2).

% dequeue all the txns in the message queue
async_dirty_dequeue(Timeout, List) ->
    receive
        {async_dirty, TmName, Txn} ->
            async_dirty_dequeue(0, [{TmName, Txn} | List]);
        Other ->
            {lists:reverse(List), Other}
        after Timeout ->
            {lists:reverse(List), undefined}
    end.

open_async_dirty_buffer_log(Log) ->
    LogFile = lists:flatten(io_lib:format("~s.~4.10.0b",
        [Log#buffer_log_send.logname, Log#buffer_log_send.fnum])),
    verbose("~s: OPEN-READ async_dirty_sender buffer log (seq=~b)",
        [Log#buffer_log_send.logname, Log#buffer_log_send.fnum]),
    case prim_file:open(mnesia2_lib:dir(LogFile), [raw, binary, read]) of
        {ok, RF} ->
            async_dirty_buffer_sender(Log#buffer_log_send{fn=LogFile, rf=RF, filepos=0});
        RError ->
            verbose("Failure opening async_dirty_sender buffer log: ~s: ~1000p",
                [LogFile, RError]),
            exit(fatal)
    end.

async_dirty_buffer_sender(#buffer_log_send{parent=Parent, rf=RF, fnum=FNum, filepos=FilePos, rtxns=[], runmode=RunMode} = Log) ->
    Runtime = timer:now_diff(os:timestamp(), Log#buffer_log_send.starttime),
    BufferSize = mnesia2_lib:val(async_dirty_buffer_size),
    %% buffer minimum run time is in milliseconds, convert it to micro
    %% since it's to be compared with the output if timer:now_diff/2
    BufferMinRuntime = mnesia2_lib:val(async_dirty_buffer_min_runtime) * 1000,
    BufferDrainedCutoff = mnesia2_lib:val(async_dirty_buffer_drained_cutoff),
    case prim_file:read(RF, BufferSize) of
        {ok, <<BufSize:32, Buf/binary>> = RBytes} ->
            if size(Buf) >= BufSize ->
               <<TxnBuf:BufSize/binary, Rest/binary>> = Buf,
               RTxns = binary_to_term(TxnBuf),
               RestSize = size(Rest),
               {ok, EofPos} = prim_file:position(RF, {eof, 0}),
               {ok, NewFilePos} = prim_file:position(RF, {bof, FilePos+size(RBytes)-RestSize}),
               NumRTxns = length(RTxns),
               NumTxns = Log#buffer_log_send.ntxns + NumRTxns,
               NewRunMode =
                if RunMode =:= running andalso
                   EofPos - NewFilePos =< (BufferDrainedCutoff * BufferSize) andalso
                   Runtime >= BufferMinRuntime ->
                       Parent ! {buffer_drained, FNum, self()},
                       verbose("~s: PRE-EOF-READ async_dirty_sender buffer log (seq=~b ntxns=~b pos=~b eof=~b remaining=~b)",
                           [Log#buffer_log_send.logname, FNum, NumTxns, NewFilePos, EofPos, EofPos - NewFilePos]),
                       eof;
               true ->
                   RunMode
                end,
               async_dirty_buffer_sender(Log#buffer_log_send{rtxns=RTxns, ntxns=NumTxns, filepos=NewFilePos, runmode=NewRunMode});
           true ->
               {ok, EofPos} = prim_file:position(RF, {eof, 0}),
               verbose("~s: ERROR: truncated read on async_dirty_sender buffer log (bytes=~b bufsize=~b size(buf)=~b pos=~b eof=~b)",
                   [Log#buffer_log_send.fn, size(RBytes), BufSize, size(Buf), FilePos, EofPos]),
               exit(shutdown)
        end;
    eof when RunMode == eof ->
        receive
        {buffer_drained_ack, _Pid} ->
            prim_file:close(RF),
            prim_file:delete(mnesia2_lib:dir(Log#buffer_log_send.fn)),
            verbose("~s: STOPPING async_dirty_sender buffer log (seq=~b ntxns=~b bytes=~b)",
                [Log#buffer_log_send.logname, FNum, Log#buffer_log_send.ntxns, Log#buffer_log_send.bytes + FilePos]),
            Parent ! {buffer_drained_ack, self()},
            exit(normal);
        {buffer_continue, FNum, _Pid} ->
            timer:sleep(100),
            async_dirty_buffer_sender(Log#buffer_log_send{runmode=running});
        {buffer_continue, _NewFNum, _Pid} ->
            prim_file:close(RF),
            prim_file:delete(mnesia2_lib:dir(Log#buffer_log_send.fn)),
            open_async_dirty_buffer_log(Log#buffer_log_send{fnum=Log#buffer_log_send.fnum+1,
                                                            bytes=Log#buffer_log_send.bytes+FilePos,
                                                            runmode=running})
        after 100 ->
            async_dirty_buffer_sender(Log)
        end;
    eof when Runtime >= BufferMinRuntime ->
        Parent ! {buffer_drained, Log#buffer_log_send.fnum, self()},
        verbose("~s: EOF-READ async_dirty_sender buffer log (seq=~b ntxns=~b eof=~b)",
            [Log#buffer_log_send.logname,
             Log#buffer_log_send.fnum,
             Log#buffer_log_send.ntxns,
             Log#buffer_log_send.filepos]),
        async_dirty_buffer_sender(Log#buffer_log_send{runmode=eof});
    eof ->
        timer:sleep(10),
        async_dirty_buffer_sender(Log);
    RError ->
        verbose("~s: async_dirty_sender buffer log read error: ~1000p",
            [Log#buffer_log_send.fn, RError]),
        exit(shutdown)
    end;
async_dirty_buffer_sender(#buffer_log_send{node=Node, rtxns=RTxns} = Log) ->
    RemTxns = send_buffer_log_try(Node, RTxns),
    if length(RemTxns) == 0 ->
       ok;
       length(RemTxns) == length(RTxns) ->
       timer:sleep(100);
    true ->
        timer:sleep(10)
    end,
    async_dirty_buffer_sender(Log#buffer_log_send{rtxns=RemTxns}).

append_buffer_log(Mode, NewTxns) ->
    append_buffer_log(Mode#buffer_log{newtxns=lists:append(Mode#buffer_log.newtxns, NewTxns)}).

append_buffer_log(Mode = #buffer_log{newtxns=[]}) ->
    Mode;
append_buffer_log(Mode = #buffer_log{wtxns=WTxns, wtxnsize=WTxnSize, newtxns=[Txn | Tail]}) ->
    TxnSize = erlang:external_size(Txn),
    BufferSize = mnesia2_lib:val(async_dirty_buffer_size),
    if WTxnSize+TxnSize+4 > BufferSize ->
       WBuf = term_to_binary(lists:reverse(WTxns)),
       WBufSize = size(WBuf),
%%     verbose("Write ~b bytes: ~p", [WBufSize, Mode#buffer_log.fn]),
       MaxBufferFileSize = mnesia2_lib:val(async_dirty_max_buffer_file_size),
       Mode1 =
        if Mode#buffer_log.fsize >= MaxBufferFileSize ->
          prim_file:close(Mode#buffer_log.wf),
          FNum = Mode#buffer_log.fnum+1,
          LogFile = lists:flatten(io_lib:format("~s.~4.10.0b", [Mode#buffer_log.fn, FNum])),
          case prim_file:open(mnesia2_lib:dir(LogFile), [raw, binary, append]) of
            {ok, WF} ->
              verbose("~s: OPEN-WRITE async_dirty_sender buffer log (seq=~b)",
                [Mode#buffer_log.fn, FNum]),
              prim_file:truncate(WF),
              Mode#buffer_log{fnum=FNum, fsize=0, wf=WF};
              OError ->
              verbose("Failure opening async_dirty_sender buffer log: ~s: ~1000p",
                [LogFile, OError]),
              exit(fatal)
          end;
        true ->
           Mode
        end,
        case prim_file:write(Mode1#buffer_log.wf, <<WBufSize:32, WBuf/binary>>) of
           ok ->
               Mode1#buffer_log{fsize=Mode1#buffer_log.fsize+WBufSize+4,
                                wtxns=[Txn],
                                wtxnsize=TxnSize,
                                newtxns=Tail};
           WError ->
               verbose("Failure writing async_dirty_sender buffer log: ~s: ~1000p",
                    [Mode1#buffer_log.fn, WError]),
               exit(fatal)
        end;
    true ->
        append_buffer_log(Mode#buffer_log{wtxns=[Txn|WTxns],
                                          wtxnsize=WTxnSize+TxnSize,
                                          newtxns=Tail})
    end.

send_buffer_log_try(_Node, []) ->
    [];
send_buffer_log_try(Node, [{TmName, Txn} | Tail] = Txns) ->
    verbose("sending txn ~p to ~p", [Txn, {TmName, Node}]),
    case erlang:send_nosuspend({TmName, Node}, Txn) of
    true ->
        send_buffer_log_try(Node, Tail);
    false ->
        Txns
    end.

check_remote_tm(Node) ->
    TmName = mnesia2_tm:num_to_async_dirty_tm_name(1),
    try sys:get_status({TmName, Node}, 10000) of
    {status, _Pid, _Mod, _Stuff} ->
        ok;
    Other ->
        verbose("bad remote mnesia2_tm response (node=~p): ~1000p~n", [Node, Other]),
        {error, invalid_response}
    catch
    _:Err ->
        verbose("** ERROR ** Possible OTP version mismatch: remote ~p not responding (node=~p): ~1000p~n",
            [TmName, Node, Err]),
        {error, Err}
    end.

node_num_to_async_dirty_tm_sender_log(Node, Num) ->
    list_to_atom("ms" ++
                 integer_to_list(((Num-1) rem ?NUM_ASYNC_DIRTY_TM_SENDER)+1) ++
                 "_"
                 ++ atom_to_list(Node)).
