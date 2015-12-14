%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1997-2011. All Rights Reserved.
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
-module(mnesia2_atomicity_test).
-author('hakan@erix.ericsson.se').
-author('rossi@erix.ericsson.se').
-compile([export_all]).
-include("mnesia2_test_lib.hrl").

init_per_testcase(Func, Conf) ->
    mnesia2_test_lib:init_per_testcase(Func, Conf).

end_per_testcase(Func, Conf) ->
    mnesia2_test_lib:end_per_testcase(Func, Conf).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
all() ->
    [explicit_abort_in_middle_of_trans,
     runtime_error_in_middle_of_trans,
     kill_self_in_middle_of_trans, throw_in_middle_of_trans,
     {group, mnesia2_down_in_middle_of_trans}].

groups() ->
    [{mnesia2_down_in_middle_of_trans, [],
      [mnesia2_down_during_infinite_trans,
       {group, lock_waiter}, {group, restart_check}]},
     {lock_waiter, [],
      [lock_waiter_sw_r, lock_waiter_sw_rt, lock_waiter_sw_wt,
       lock_waiter_wr_r, lock_waiter_srw_r, lock_waiter_sw_sw,
       lock_waiter_sw_w, lock_waiter_sw_wr, lock_waiter_sw_srw,
       lock_waiter_wr_wt, lock_waiter_srw_wt,
       lock_waiter_wr_sw, lock_waiter_srw_sw, lock_waiter_wr_w,
       lock_waiter_srw_w, lock_waiter_r_sw, lock_waiter_r_w,
       lock_waiter_r_wt, lock_waiter_rt_sw, lock_waiter_rt_w,
       lock_waiter_rt_wt, lock_waiter_wr_wr,
       lock_waiter_srw_srw, lock_waiter_wt_r, lock_waiter_wt_w,
       lock_waiter_wt_rt, lock_waiter_wt_wt, lock_waiter_wt_wr,
       lock_waiter_wt_srw, lock_waiter_wt_sw, lock_waiter_w_wr,
       lock_waiter_w_srw, lock_waiter_w_sw, lock_waiter_w_r,
       lock_waiter_w_w, lock_waiter_w_rt, lock_waiter_w_wt]},
     {restart_check, [],
      [restart_r_one, restart_w_one, restart_rt_one,
       restart_wt_one, restart_wr_one, restart_sw_one,
       restart_r_two, restart_w_two, restart_rt_two,
       restart_wt_two, restart_wr_two, restart_sw_two]}].

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, Config) ->
    Config.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
explicit_abort_in_middle_of_trans(suite) -> [];
explicit_abort_in_middle_of_trans(Config) when is_list(Config) ->
    [Node1] = Nodes = ?acquire_nodes(1, Config),
    Tab = explicit_abort_in_middle_of_trans,

    Rec1A = {Tab, 1, a},
    Rec1B = {Tab, 1, b},

    ?match({atomic, ok}, mnesia2:create_table([{name, Tab},
					     {ram_copies, [Node1]}])),
    %% Start a transaction on one node
    {success, [A]} = ?start_activities([Node1]),

    %% store an object in the Tab - first tranaction
    ?start_transactions([A]),
    A ! fun() ->
		mnesia2:write(Rec1A)	% returns ok when successful
	 end,
    ?match_receive({A, ok}),
    A ! end_trans,
    ?match_receive({A, {atomic, end_trans}}),

    %% second transaction: store some new objects and abort before the
    %% transaction is finished -> the new changes should be invisable
    ?start_transactions([A]),
    A ! fun() ->
		mnesia2:write(Rec1B),
		exit(abort_by_purpose) %does that stop the process A ???
	 end,
    ?match_receive({A, {aborted, abort_by_purpose}}),


    %?match_receive({A, {'EXIT', Pid, normal}}), % A died and sends EXIT


    %% Start a second  transactionprocess, after the first failed
    {success, [B]} = ?start_activities([Node1]),

    %% check, whether the interupted transaction had no influence on the db
    ?start_transactions([B]),
    B ! fun() ->
		?match([Rec1A], mnesia2:read({Tab, 1})),
		ok
	 end,
    ?match_receive({B, ok}),
    B ! end_trans,
    ?match_receive({B, {atomic, end_trans}}),

    ?verify_mnesia2(Nodes, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
runtime_error_in_middle_of_trans(suite) -> [];
runtime_error_in_middle_of_trans(Config) when is_list(Config) ->
    [Node1] = Nodes = ?acquire_nodes(1, Config),
    Tab = runtime_error_in_middle_of_trans,

    Rec1A = {Tab, 1, a},
    Rec1B = {Tab, 1, b},
    Rec1C = {Tab, 1, c},

    ?match({atomic, ok}, mnesia2:create_table([{name, Tab},
					     {ram_copies, [Node1]}])),
    %% Start a transaction on one node
    {success, [A]} = ?start_activities([Node1]),

    %% store an object in the Tab - first tranaction
    ?start_transactions([A]),
    A ! fun() ->
		mnesia2:write(Rec1A)	% returns ok when successful
	 end,
    ?match_receive({A, ok}),
    A ! end_trans,
    ?match_receive({A, {atomic, end_trans}}),

    %% second transaction: store some new objects and abort before the
    %% transaction is finished -> the new changes should be invisable
    ?start_transactions([A]),
    A ! fun() ->
		mnesia2:write(Rec1B),
                erlang:error(foo), % that should provoke a runtime error
		mnesia2:write(Rec1C)
	 end,
    ?match_receive({A, {aborted, _Reason}}),

    %?match_receive({A, {'EXIT', Msg1}), % A died and sends EXIT


    %% Start a second  transactionprocess, after the first failed
    {success, [B]} = ?start_activities([Node1]),

    %% check, whether the interupted transaction had no influence on the db
    ?start_transactions([B]),
    B ! fun() ->
		?match([Rec1A], mnesia2:read({Tab, 1})),
		ok
	 end,
    ?match_receive({B, ok}),
    B ! end_trans,
    ?match_receive({B, {atomic, end_trans}}),

    ?verify_mnesia2(Nodes, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
kill_self_in_middle_of_trans(suite) -> [];
kill_self_in_middle_of_trans(Config) when is_list(Config) ->
    [Node1] = Nodes = ?acquire_nodes(1, Config),
    Tab = kill_self_in_middle_of_trans,

    Rec1A = {Tab, 1, a},
    Rec1B = {Tab, 1, b},
    Rec1C = {Tab, 1, c},

    ?match({atomic, ok}, mnesia2:create_table([{name, Tab},
					     {ram_copies, [Node1]}])),
    %% Start a transaction on one node
    {success, [A]} = ?start_activities([Node1]),

    %% store an object in the Tab - first tranaction
    ?start_transactions([A]),
    A ! fun() ->
		mnesia2:write(Rec1A)	% returns ok when successful
	 end,
    ?match_receive({A, ok}),
    A ! end_trans,
    ?match_receive({A, {atomic, end_trans}}),

    %% second transaction: store some new objects and abort before the
    %% transaction is finished -> the new changes should be invisable
    ?start_transactions([A]),
    A ! fun() ->
		mnesia2:write(Rec1B),
                exit(self(), kill), % that should kill the process himself
		                        %   - poor guy !
		mnesia2:write(Rec1C)
	 end,
    %%
    %% exit(.., kill) : the transaction can't trap this error - thus no
    %%                 proper result can be send by the test server

    % ?match_receive({A, {aborted, Reason}}),

    ?match_receive({'EXIT', _Pid, killed}), % A is killed and sends EXIT

    %% Start a second  transactionprocess, after the first failed
    {success, [B]} = ?start_activities([Node1]),

    %% check, whether the interupted transaction had no influence on the db
    ?start_transactions([B]),
    B ! fun() ->
		?match([Rec1A], mnesia2:read({Tab, 1})),
		ok
	 end,
    ?match_receive({B, ok}),
    B ! end_trans,
    ?match_receive({B, {atomic, end_trans}}),

    ?verify_mnesia2(Nodes, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
throw_in_middle_of_trans(suite) -> [];
throw_in_middle_of_trans(Config) when is_list(Config) ->
    [Node1] = Nodes = ?acquire_nodes(1, Config),
    Tab = throw_in_middle_of_trans,

    Rec1A = {Tab, 1, a},
    Rec1B = {Tab, 1, b},
    Rec1C = {Tab, 1, c},

    ?match({atomic, ok}, mnesia2:create_table([{name, Tab},
					     {ram_copies, [Node1]}])),
    %% Start a transaction on one node
    {success, [A]} = ?start_activities([Node1]),

    %% store an object in the Tab - first tranaction
    ?start_transactions([A]),
    A ! fun() ->
		mnesia2:write(Rec1A)	% returns ok when successful
	 end,
    ?match_receive({A, ok}),
    A ! end_trans,
    ?match_receive({A, {atomic, end_trans}}),

    %% second transaction: store some new objects and abort before the
    %% transaction is finished -> the new changes should be invisable
    ?start_transactions([A]),
    A ! fun() ->
		mnesia2:write(Rec1B),
                throw(exit_transactian_by_a_throw),
		mnesia2:write(Rec1C)
	 end,
    ?match_receive({A, {aborted, {throw, exit_transactian_by_a_throw}}}),
    % A ! end_trans, % is A still alive ?
    % ?match_receive({A, {atomic, end_trans}}), % {'EXIT', Pid, normal}

    %?match_receive({A, {'EXIT', Pid, normal}}), % A died and sends EXIT

    %% Start a second  transactionprocess, after the first failed
    {success, [B]} = ?start_activities([Node1]),

    %% check, whether the interupted transaction had no influence on the db
    ?start_transactions([B]),
    B ! fun() ->
		?match([Rec1A], mnesia2:read({Tab, 1})),
		ok
	 end,
    ?match_receive({B, ok}),
    B ! end_trans,
    ?match_receive({B, {atomic, end_trans}}),

    ?verify_mnesia2(Nodes, []).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
mnesia2_down_during_infinite_trans(suite) -> [];
mnesia2_down_during_infinite_trans(Config) when is_list(Config) ->
    [Node1, Node2]  = ?acquire_nodes(2, Config),
    Tab = mnesia2_down_during_infinite_trans,

    ?match({atomic, ok},
	   mnesia2:create_table([{name, Tab}, {ram_copies, [Node1, Node2]}])),
    %% Start a transaction on one node
    {success, [A2, A1]} = ?start_activities([Node2, Node1]),
    %% Start order of the transactions are important
    %% We also needs to sync the tid counter
    ?match({atomic, ok},
	   mnesia2:transaction(fun() -> mnesia2:write({Tab, 1, test_ok}) end)),
    mnesia2_test_lib:start_sync_transactions([A2, A1]),

    %% Obtain a write lock and wait forever
    RecA = {Tab, 1, test_not_ok},
    A1 ! fun() -> mnesia2:write(RecA) end,
    ?match_receive({A1, ok}),

    A1 ! fun() -> process_flag(trap_exit, true), timer:sleep(infinity) end,
    ?match_receive(timeout),

    %% Try to get read lock, but gets queued
    A2 ! fun() -> mnesia2:read({Tab, 1}) end,
    ?match_receive(timeout),

    %% Kill mnesia2 on other node
    mnesia2_test_lib:kill_mnesia2([Node1]),

    %% Second transaction gets the read lock
    ?match_receive({A2, [{Tab, 1, test_ok}]}),
    exit(A1, kill), % Needed since we trap exit

    ?verify_mnesia2([Node2], [Node1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

lock_waiter_sw_r(suite) -> [];
lock_waiter_sw_r(Config) when is_list(Config) ->
    start_lock_waiter(sw, r, Config).

lock_waiter_sw_rt(suite) -> [];
lock_waiter_sw_rt(Config) when is_list(Config) ->
    start_lock_waiter(sw, rt, Config).

lock_waiter_sw_wt(suite) -> [];
lock_waiter_sw_wt(Config) when is_list(Config) ->
    start_lock_waiter(sw, wt,Config).

lock_waiter_wr_r(suite) -> [];
lock_waiter_wr_r(Config) when is_list(Config) ->
    start_lock_waiter(wr, r, Config).

lock_waiter_srw_r(suite) -> [];
lock_waiter_srw_r(Config) when is_list(Config) ->
    start_lock_waiter(srw, r, Config).

lock_waiter_sw_sw(suite) -> [];
lock_waiter_sw_sw(Config) when is_list(Config) ->
    start_lock_waiter(sw, sw,Config).

lock_waiter_srw_srw(suite) -> [];
lock_waiter_srw_srw(Config) when is_list(Config) ->
    start_lock_waiter(srw, srw,Config).

lock_waiter_wr_wr(suite) -> [];
lock_waiter_wr_wr(Config) when is_list(Config) ->
    start_lock_waiter(wr, wr,Config).

lock_waiter_sw_w(suite) -> [];
lock_waiter_sw_w(Config) when is_list(Config) ->
    start_lock_waiter(sw, w,Config).

lock_waiter_sw_wr(suite) -> [];
lock_waiter_sw_wr(Config) when is_list(Config) ->
    start_lock_waiter(sw, wr,Config).

lock_waiter_sw_srw(suite) -> [];
lock_waiter_sw_srw(Config) when is_list(Config) ->
    start_lock_waiter(sw, srw,Config).

lock_waiter_wr_wt(suite) -> [];
lock_waiter_wr_wt(Config) when is_list(Config) ->
    start_lock_waiter(wr, wt,Config).

lock_waiter_srw_wt(suite) -> [];
lock_waiter_srw_wt(Config) when is_list(Config) ->
    start_lock_waiter(srw, wt,Config).

lock_waiter_wr_sw(suite) -> [];
lock_waiter_wr_sw(Config) when is_list(Config) ->
    start_lock_waiter(wr, sw,Config).

lock_waiter_srw_sw(suite) -> [];
lock_waiter_srw_sw(Config) when is_list(Config) ->
    start_lock_waiter(srw, sw,Config).

lock_waiter_wr_w(suite) -> [];
lock_waiter_wr_w(Config) when is_list(Config) ->
    start_lock_waiter(wr, w,Config).

lock_waiter_srw_w(suite) -> [];
lock_waiter_srw_w(Config) when is_list(Config) ->
    start_lock_waiter(srw, w,Config).

lock_waiter_r_sw(suite) -> [];
lock_waiter_r_sw(Config) when is_list(Config) ->
    start_lock_waiter(r, sw,Config).

lock_waiter_r_w(suite) -> [];
lock_waiter_r_w(Config) when is_list(Config) ->
    start_lock_waiter(r, w,Config).

lock_waiter_r_wt(suite) -> [];
lock_waiter_r_wt(Config) when is_list(Config) ->
    start_lock_waiter(r, wt,Config).

lock_waiter_rt_sw(suite) -> [];
lock_waiter_rt_sw(Config) when is_list(Config) ->
    start_lock_waiter(rt, sw,Config).

lock_waiter_rt_w(suite) -> [];
lock_waiter_rt_w(Config) when is_list(Config) ->
     start_lock_waiter(rt, w,Config).

lock_waiter_rt_wt(suite) -> [];
lock_waiter_rt_wt(Config) when is_list(Config) ->
    start_lock_waiter(rt, wt,Config).

lock_waiter_wt_r(suite) -> [];
lock_waiter_wt_r(Config) when is_list(Config) ->
    start_lock_waiter(wt, r,Config).

lock_waiter_wt_w(suite) -> [];
lock_waiter_wt_w(Config) when is_list(Config) ->
    start_lock_waiter(wt, w,Config).

lock_waiter_wt_rt(suite) -> [];
lock_waiter_wt_rt(Config) when is_list(Config) ->
    start_lock_waiter(wt, rt,Config).

lock_waiter_wt_wt(suite) -> [];
lock_waiter_wt_wt(Config) when is_list(Config) ->
    start_lock_waiter(wt, wt,Config).

lock_waiter_wt_wr(suite) -> [];
lock_waiter_wt_wr(Config) when is_list(Config) ->
    start_lock_waiter(wt, wr,Config).

lock_waiter_wt_srw(suite) -> [];
lock_waiter_wt_srw(Config) when is_list(Config) ->
    start_lock_waiter(wt, srw,Config).

lock_waiter_wt_sw(suite) -> [];
lock_waiter_wt_sw(Config) when is_list(Config) ->
    start_lock_waiter(wt, sw,Config).

lock_waiter_w_wr(suite) -> [];
lock_waiter_w_wr(Config) when is_list(Config) ->
    start_lock_waiter(w, wr, Config).

lock_waiter_w_srw(suite) -> [];
lock_waiter_w_srw(Config) when is_list(Config) ->
    start_lock_waiter(w, srw, Config).

lock_waiter_w_sw(suite) -> [];
lock_waiter_w_sw(Config) when is_list(Config) ->
    start_lock_waiter(w, sw, Config).

lock_waiter_w_r(suite) -> [];
lock_waiter_w_r(Config) when is_list(Config) ->
    start_lock_waiter(w, r, Config).

lock_waiter_w_w(suite) -> [];
lock_waiter_w_w(Config) when is_list(Config) ->
    start_lock_waiter(w, w, Config).

lock_waiter_w_rt(suite) -> [];
lock_waiter_w_rt(Config) when is_list(Config) ->
    start_lock_waiter(w, rt, Config).

lock_waiter_w_wt(suite) -> [];
lock_waiter_w_wt(Config) when is_list(Config) ->
    start_lock_waiter(w, wt, Config).

start_lock_waiter(BlockOpA, BlockOpB, Config) ->
    [N1, N2] = Nodes = ?acquire_nodes(2, Config),

    TabName = mk_tab_name(lock_waiter_),
    ?match({atomic, ok}, mnesia2:create_table(TabName,
					     [{ram_copies, [N1, N2]}])),

    %% initialize the table with object {1, c} - when there
    %% is a read transaction, the read will find that  value
    ?match({atomic, ok}, mnesia2:sync_transaction(fun() -> mnesia2:write({TabName, 1, c}) end)),
    rpc:call(N2, ?MODULE, sync_tid_release, []),

    Tester = self(),
    Fun_A =fun() ->
		   NewCounter = incr_restart_counter(),
		   if
		       NewCounter == 1 ->
			   Tester ! go_ahead_test,
			   receive go_ahead -> ok end;
		      true -> ok
		   end,
		   lock_waiter_fun(BlockOpA, TabName, a),
		   NewCounter
	   end,

    %% it's not possible to just spawn the transaction, because
    %% the result shall be evaluated
    A = spawn_link(N1, ?MODULE, perform_restarted_transaction, [Fun_A]),

    ?match(ok, receive go_ahead_test -> ok after 10000 -> timeout end),

    mnesia2_test_lib:sync_trans_tid_serial([N1, N2]),

    Fun_B = fun() ->
		    lock_waiter_fun(BlockOpB, TabName, b),
		    A ! go_ahead,
		    wait(infinity)
	    end,

    B = spawn_link(N2, mnesia2, transaction, [Fun_B, 100]),

    io:format("waiting for A (~p on ~p) to be in the queue ~n", [A, [N1, N2]]),
    wait_for_a(A, [N1, N2]),

    io:format("Queus ~p~n",
	      [[{N,rpc:call(N, mnesia2, system_info, [lock_queue])} || N <- Nodes]]),

    KillNode = node(B),
    io:format("A was in the queue, time to kill Mnesia2 on B's node (~p on ~p)~n",
	      [B, KillNode]),

    mnesia2_test_lib:kill_mnesia2([KillNode]), % kill mnesia2 of fun B

    %% Read Ops does not need to be restarted
    ExpectedCounter =
	if
	    BlockOpA == sw, BlockOpB == w -> 1;
	    BlockOpA == sw, BlockOpB == wt -> 1;
	    BlockOpA == sw, BlockOpB == wr -> 1;
	    BlockOpA == srw, BlockOpB == w -> 1;
	    BlockOpA == srw, BlockOpB == wt -> 1;
	    BlockOpA == srw, BlockOpB == wr -> 1;
	    BlockOpA == r,  BlockOpB /= sw -> 1;
	    BlockOpA == rt, BlockOpB /= sw -> 1;
	    true -> 2
	end,
    receive {'EXIT', B, _} -> ok
    after 3000 -> ?error("Timeout~n", []) end,
    receive {'EXIT', A, Exp1} -> ?match({atomic, ExpectedCounter}, Exp1)
    after 3000 -> ?error("Timeout~n", []) end,

    %% the expected result depends on the transaction of
    %% fun A - when that doesn't change the object in the
    %% table (e.g. it is a read) then the predefined
    %% value {Tabname, 1, c} is expected to be the result here
    ExpectedResult =
	case BlockOpA of
	    w -> {TabName, 1, a};
	    sw ->{TabName, 1, a};
	    _all_other -> {TabName, 1, c}
	end,

    ?match({atomic, [ExpectedResult]},
	   mnesia2:transaction(fun() -> mnesia2:read({TabName, 1}) end, 100)),
    ?verify_mnesia2([N1], [N2]).

mk_tab_name(Prefix) ->
    Count = erlang:unique_integer([monotonic,positive]),
    list_to_atom(lists:concat([Prefix , '_', Count])).

lock_waiter_fun(Op, TabName, Val) ->
    case Op of
	rt ->  mnesia2:read_lock_table(TabName);
	wt ->  mnesia2:write_lock_table(TabName);
	r  ->  mnesia2:read({TabName, 1});
	w ->   mnesia2:write({TabName, 1, Val});
	wr ->  mnesia2:wread({TabName, 1});
	srw -> mnesia2:read(TabName, 1, sticky_write);
	sw ->  mnesia2:s_write({TabName, 1, Val})
    end.

wait_for_a(Pid, Nodes) ->
    wait_for_a(Pid, Nodes, 5).

wait_for_a(_P, _N, 0) ->
    ?error("Timeout while waiting for lock on a~n", []);

wait_for_a(Pid, Nodes, Count) ->
    %%    io:format("WAIT_FOR_A ~p ON ~w ~n", [Pid, Nodes]),
    List = [rpc:call(N, mnesia2, system_info, [lock_queue]) || N <- Nodes],
    Q = lists:append(List),
    check_q(Pid, Q, Nodes, Count).

check_q(Pid, [{{_Oid,_Tid}, _Op, Pid, _WFT} | _Tail], _N, _Count) ->
    ok;
check_q(Pid, [{_Oid, _Op, Pid, _Tid, _WFT} | _Tail], _N, _Count) ->
    ok;
check_q(Pid, [_ | Tail], N, Count) ->
    check_q(Pid, Tail, N, Count);
check_q(Pid, [], N, Count) ->
    timer:sleep(500),
    wait_for_a(Pid, N, Count - 1).

perform_restarted_transaction (Fun_Trans) ->
    %% the result of the transaction shall be:
    %%  - undefined (if the transaction was never executed)
    %%  - Times ( number of times that the transaction has been executed)

    Result = mnesia2:transaction(Fun_Trans, 100),
    exit(Result).

%% Returns new val
incr_restart_counter() ->
    NewCount =
	case get(count_restart_of_transaction) of
	    undefined -> 1;
	    OldCount -> OldCount + 1
	end,
    put(count_restart_of_transaction, NewCount),
    NewCount.

wait(Mseconds) ->
    receive
    after Mseconds -> ok
    end.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

restart_r_one(suite) -> [];
restart_r_one(Config) when is_list(Config) ->
    start_restart_check(r, one, Config).

restart_w_one(suite) -> [];
restart_w_one(Config) when is_list(Config) ->
    start_restart_check(w, one, Config).

restart_rt_one(suite) -> [];
restart_rt_one(Config) when is_list(Config) ->
    start_restart_check(rt, one, Config).

restart_wt_one(suite) -> [];
restart_wt_one(Config) when is_list(Config) ->
    start_restart_check(wt, one, Config).

restart_wr_one(suite) -> [];
restart_wr_one(Config) when is_list(Config) ->
    start_restart_check(wr, one, Config).

restart_sw_one(suite) -> [];
restart_sw_one(Config) when is_list(Config) ->
    start_restart_check(sw, one, Config).

restart_r_two(suite) -> [];
restart_r_two(Config) when is_list(Config) ->
    start_restart_check(r, two, Config).

restart_w_two(suite) -> [];
restart_w_two(Config) when is_list(Config) ->
    start_restart_check(w, two, Config).

restart_rt_two(suite) -> [];
restart_rt_two(Config) when is_list(Config) ->
    start_restart_check(rt, two, Config).

restart_wt_two(suite) -> [];
restart_wt_two(Config) when is_list(Config) ->
    start_restart_check(wt, two, Config).

restart_wr_two(suite) -> [];
restart_wr_two(Config) when is_list(Config) ->
    start_restart_check(wr, two, Config).

restart_sw_two(suite) -> [];
restart_sw_two(Config) when is_list(Config) ->
    start_restart_check(sw, two, Config).

start_restart_check(RestartOp, ReplicaNeed, Config) ->
    [N1, N2, N3] = Nodes = ?acquire_nodes(3, Config),

    {TabName, _TabNodes} = create_restart_table(ReplicaNeed, Nodes),

    %% initialize the table with object {1, c} - when there
    %% is a read transaction, the read will find that  value
    ?match({atomic, ok}, mnesia2:sync_transaction(fun() -> mnesia2:write({TabName, 1, c}) end)),

    %% Really sync tid_release
    rpc:multicall([N2,N3], ?MODULE, sync_tid_release, []),
    Coord = self(),

    Fun_A = fun() ->
		    NewCounter = incr_restart_counter(),
		    case NewCounter of
			1 ->
			    mnesia2:write({TabName, 1, d}),
			    %% send a message to the test proc
			    Coord ! {self(),fun_a_is_blocked},
			    receive go_ahead -> ok end;
			_ ->
			    %% the fun will NOT be blocked here
			    restart_fun_A(RestartOp, TabName)
		    end,
		    NewCounter
	    end,

    A = spawn_link(N1, ?MODULE, perform_restarted_transaction, [Fun_A]),
    ?match_receive({A,fun_a_is_blocked}),

    %% mnesia2 shall be killed at that node, where A is reading
    %% the information from
    kill_where_to_read(TabName, N1, [N2, N3]),

    %% wait some time to let mnesia2 go down and spread those news around
    %% fun A shall be able to finish its job before being restarted
    wait(500),
    A ! go_ahead,

    %% the sticky write doesnt work on remote nodes !!!
    ExpectedMsg =
	case RestartOp of
	    sw when ReplicaNeed == two ->
		{'EXIT',A,{aborted,  {not_local, TabName}}};
	    _all_other ->
		case ReplicaNeed of
		    one ->
			{'EXIT',A,{aborted,  {no_exists, TabName}}};
		    two ->
			{'EXIT',A,{atomic, 2}}
		end
	end,

    ?match_receive(ExpectedMsg),

    %% now mnesia2 has to be started again on the node KillNode
    %% because the next test suite will need it
    ?match([], mnesia2_test_lib:start_mnesia2(Nodes, [TabName])),


    %%  the expected result depends on the transaction of
    %%  fun A - when that doesnt change the object in the
    %%  table (e.g. it is a read) then the predefined
    %%  value {Tabname, 1, c} is expected to be the result here

    ExpectedResult =
	case ReplicaNeed of
	    one ->
		[];
	    two ->
		case RestartOp of
		    w -> [{TabName, 1, a}];
		    _ ->[ {TabName, 1, c}]
		end
	end,

    ?match({atomic, ExpectedResult},
	   mnesia2:transaction(fun() -> mnesia2:read({TabName, 1}) end,100)),
    ?verify_mnesia2(Nodes, []).

create_restart_table(ReplicaNeed, [_N1, N2, N3]) ->
    TabNodes =
	case ReplicaNeed of
	    one ->  [N2];
	    two ->  [N2, N3]
	end,
    TabName = mk_tab_name(restart_check_),
    ?match({atomic, ok}, mnesia2:create_table(TabName, [{ram_copies, TabNodes}])),
    {TabName, TabNodes}.

restart_fun_A(Op, TabName) ->
    case Op of
	rt -> mnesia2:read_lock_table(TabName);
	wt -> mnesia2:write_lock_table(TabName);
	r ->  mnesia2:read( {TabName, 1});
	w ->  mnesia2:write({TabName, 1, a});
	wr -> mnesia2:wread({TabName, 1});
	sw -> mnesia2:s_write({TabName, 1, a})
    end.

kill_where_to_read(TabName, N1, Nodes) ->
    Read = rpc:call(N1,mnesia2,table_info, [TabName, where_to_read]),
    case lists:member(Read, Nodes) of
	true ->
	    mnesia2_test_lib:kill_mnesia2([Read]);
	false ->
	    ?error("Fault while killing Mnesia2: ~p~n", [Read]),
	    mnesia2_test_lib:kill_mnesia2(Nodes)
    end.

sync_tid_release() ->
    sys:get_status(whereis(mnesia2_tm)),
    sys:get_status(whereis(mnesia2_locker)),
    ok.

