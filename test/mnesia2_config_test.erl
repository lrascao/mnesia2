%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1997-2014. All Rights Reserved.
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
-module(mnesia2_config_test).
-author('hakan@erix.ericsson.se').

-include("mnesia2_test_lib.hrl").
 
-record(test_table,{i,a1,a2,a3}).
-record(test_table2,{i, b}).

-export([
	all/0,groups/0,init_per_group/2,end_per_group/2,
	 access_module/1,
	 auto_repair/1,
	 backup_module/1,
	 debug/1,
	 dir/1,
	 dump_log_load_regulation/1,
	
	 dump_log_update_in_place/1,
	 event_module/1,
	 inconsistent_database/1,
	 max_wait_for_decision/1,
	 send_compressed/1,

	 app_test/1,
	
	 schema_merge/1,
	 unknown_config/1,

	 dump_log_time_threshold/1,
	 dump_log_write_threshold/1,
	 
	 start_one_disc_full_then_one_disc_less/1,
	 start_first_one_disc_less_then_one_disc_full/1,
	 start_first_one_disc_less_then_two_more_disc_less/1,
	 schema_location_and_extra_db_nodes_combinations/1,
	 table_load_to_disc_less_nodes/1,
	
	 dynamic_basic/1,
	 dynamic_ext/1,
	 dynamic_bad/1,

	 init_per_testcase/2,
	 end_per_testcase/2,
	 c_nodes/0
	]).

-export([check_logs/1]).

-define(init(N, Config),
	mnesia2_test_lib:prepare_test_case([{init_test_case, [mnesia2]},
					   delete_schema,
					   {reload_appls, [mnesia2]}],
					  N, Config, ?FILE, ?LINE)).
-define(acquire(N, Config),
	mnesia2_test_lib:prepare_test_case([{init_test_case, [mnesia2]},
					   delete_schema,
					   {reload_appls, [mnesia2]},
					   create_schema,
					   {start_appls, [mnesia2]}],
					  N, Config, ?FILE, ?LINE)).
-define(acquire_schema(N, Config),
	mnesia2_test_lib:prepare_test_case([{init_test_case, [mnesia2]},
					    delete_schema,
					    {reload_appls, [mnesia2]},
					    create_schema],
					  N, Config, ?FILE, ?LINE)).
-define(cleanup(N, Config),
	mnesia2_test_lib:prepare_test_case([{reload_appls, [mnesia2]}],
					  N, Config, ?FILE, ?LINE)).
-define(trans(Fun),
	?match({atomic, ok}, mnesia2:transaction(Fun))).

init_per_testcase(Func, Conf) ->
    mnesia2_test_lib:init_per_testcase(Func, Conf).

end_per_testcase(Func, Conf) ->
    mnesia2_test_lib:end_per_testcase(Func, Conf).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%


all() -> 
    [access_module, auto_repair, backup_module, debug, dir,
     dump_log_load_regulation, {group, dump_log_thresholds},
     dump_log_update_in_place,
     event_module,
     inconsistent_database, max_wait_for_decision,
     send_compressed, app_test, {group, schema_config},
     unknown_config].

groups() -> 
    [{dump_log_thresholds, [],
      [dump_log_time_threshold, dump_log_write_threshold]},
     {schema_config, [],
      [start_one_disc_full_then_one_disc_less,
       start_first_one_disc_less_then_one_disc_full,
       start_first_one_disc_less_then_two_more_disc_less,
       schema_location_and_extra_db_nodes_combinations,
       table_load_to_disc_less_nodes, schema_merge,
       {group, dynamic_connect}]},
     {dynamic_connect, [],
      [dynamic_basic, dynamic_ext, dynamic_bad]}].

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, Config) ->
    Config.



%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

access_module(doc) ->
    ["Replace the activity access module with another module and ",
     "use it to read and write to some alternate table storage"];
access_module(suite) -> [];
access_module(Config) when is_list(Config) ->
    Nodes = ?acquire_schema(1, Config),
    ?match(ok, mnesia2:start([{access_module, mnesia2_frag}])),

    ?match(mnesia2_frag, mnesia2:system_info(access_module)),

    access_tab(ram_copies, Nodes),
    case mnesia2_test_lib:diskless(Config) of
	true -> skip;
	false -> 
	    access_tab(disc_copies, Nodes)
		,  access_tab(disc_only_copies, Nodes)
    end,

    ?verify_mnesia2(Nodes, []),
    ?cleanup(1, Config).

access_tab(Storage, Nodes) ->
    Tab = list_to_atom(lists:concat([access_tab_, Storage])),
    RecName = some_access,
    Attr = val,
    TabDef = [{Storage, Nodes},
	      {type, bag},
	      {index, [Attr]},
	      {record_name, RecName}],
    ?match({atomic,ok}, mnesia2:create_table(Tab, TabDef)),

    Activity = fun(Kind) ->
		       A = [Kind, Tab, RecName, Attr, Nodes],
		       io:format("kind: ~w, storage: ~w~n", [Kind, Storage]),
		       mnesia2:activity(Kind, fun do_access/5, A)
	       end,
    ModActivity = fun(Kind, M) ->
			  io:format("kind: ~w, storage: ~w. module: ~w~n",
				    [Kind, Storage, M]),
			  A = [Kind, Tab, RecName, Attr, Nodes],
			  mnesia2:activity(Kind, fun do_access/5, A, M)
	       end,
    ?match(ok, Activity(transaction)),
    ?match(ok, Activity({transaction, 47})),
    ?match(ok, ModActivity(transaction, mnesia2)),
    ?match(ok, ModActivity(transaction, mnesia2_frag)),
    
    ?match(ok, Activity(async_dirty)),
    ?match(ok, Activity(sync_dirty)),
    case Storage of
	ram_copies ->
	    ?match(ok, Activity(ets));
	_ ->
	    ignore
    end.

do_access(Kind, Tab, RecName, Attr, Nodes) ->
    Tens = lists:sort([{RecName, 1, 10}, {RecName, 3, 10}]),
    {OptNodes, OptTens} = 
	case Kind of
	    transaction -> {Nodes, Tens};
	    {transaction, _} -> {Nodes, Tens};
	    async_dirty -> {[], Tens};
	    sync_dirty -> {[], Tens};
	    ets -> {[], []}
	end,
    ?match(RecName, mnesia2:table_info(Tab, record_name)),
    
    ?match(ok, mnesia2:write(Tab, {RecName, 1, 10}, write)),
    ?match(ok, mnesia2:write(Tab, {RecName, 2, 20}, sticky_write)),
    ?match(ok, mnesia2:write(Tab, {RecName, 2, 21}, sticky_write)),
    ?match(ok, mnesia2:write(Tab, {RecName, 2, 22}, write)),
    ?match(ok, mnesia2:write(Tab, {RecName, 3, 10}, write)),

    Twos = [{RecName, 2, 20}, {RecName, 2, 21}, {RecName, 2, 22}],
    ?match(Twos, lists:sort(mnesia2:read(Tab, 2, read))),
    
    ?match(ok, mnesia2:delete_object(Tab, {RecName, 2, 21}, sticky_write)),

    TenPat = {RecName, '_', 10},
    ?match(Tens, lists:sort(mnesia2:match_object(Tab, TenPat, read))),
    ?match(OptTens, lists:sort(mnesia2:index_match_object(Tab, TenPat, Attr, read) )),
    ?match(OptTens, lists:sort(mnesia2:index_read(Tab, 10, Attr))),
    Keys = [1, 2, 3],
    ?match(Keys, lists:sort(mnesia2:all_keys(Tab))),

    First = mnesia2:first(Tab),
    Mid   = mnesia2:next(Tab, First),
    Last  = mnesia2:next(Tab, Mid),
    ?match('$end_of_table', mnesia2:next(Tab, Last)),
    ?match(Keys, lists:sort([First,Mid,Last])),

    %% For set and bag these last, prev works as first and next
    First2 = mnesia2:last(Tab),
    Mid2   = mnesia2:prev(Tab, First2),
    Last2  = mnesia2:prev(Tab, Mid2),
    ?match('$end_of_table', mnesia2:prev(Tab, Last2)),
    ?match(Keys, lists:sort([First2,Mid2,Last2])),

    ?match([ok, ok, ok], [mnesia2:delete(Tab, K, write) || K <- Keys]),
    W = wild_pattern,
    ?match([], mnesia2:match_object(Tab, mnesia2:table_info(Tab, W), read)),
    ?log("Safe fixed ~p~n", [catch ets:info(Tab, safe_fixed)]),
    ?log("Fixed ~p ~n", [catch ets:info(Tab, fixed)]),
    
    ?match(OptNodes, mnesia2:lock({global, some_lock_item, Nodes}, write)),
    ?match(OptNodes, mnesia2:lock({global, some_lock_item, Nodes}, read)),
    ?match(OptNodes, mnesia2:lock({table, Tab}, read)),
    ?match(OptNodes, mnesia2:lock({table, Tab}, write)),
    
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
auto_repair(doc) ->
    ["Try the auto_repair mechanism on the various disk_logs and dets files.",
     "",
     "The case tests both normal values of the parameter, and also",
     "one crazy value.",
     "The test of the real auto_repair functionality is made in the",
     "dets suite"
    ];
auto_repair(suite) -> [];
auto_repair(Config) when is_list(Config) ->
    ?init(1, Config),
    ?match(ok, mnesia2:start()),			% Check default true
    ?match(true, mnesia2:system_info(auto_repair)),
    ?match(stopped, mnesia2:stop()),
    ?match(ok, mnesia2:start([{auto_repair, true}])),
    ?match(true, mnesia2:system_info(auto_repair)),
    ?match(stopped, mnesia2:stop()),
    ?match(ok, mnesia2:start([{auto_repair, false}])),
    ?match(false, mnesia2:system_info(auto_repair)),
    ?match(stopped, mnesia2:stop()),
    ?match({error, {bad_type, auto_repair, your_mama}},
	   mnesia2:start([{auto_repair, your_mama}])),
     ?match(stopped, mnesia2:stop()),
    ?cleanup(1, Config),
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

backup_module(doc) ->
    ["Replace the backup module with another module and use it to",
     "read and write to an alternate backup media, e.g stored in",
     "the internal state of a simple process."];
backup_module(suite) -> [];
backup_module(Config) when is_list(Config) ->
    Nodes = ?acquire_schema(1, Config),
    ?match(ok, mnesia2:start([{backup_module, mnesia2_config_backup}])),
    ?match({atomic,ok},
	   mnesia2:create_table(test_table,
			       [{disc_copies, Nodes},
				{attributes,
				 record_info(fields,test_table)}])),
    
    ?match({atomic,ok},
	   mnesia2:create_table(test_table2,
			       [{disc_copies, Nodes},
				{attributes,
				 record_info(fields,test_table2)}])),
    %% Write in test table 
    ?trans(fun() -> mnesia2:write(#test_table{i=1}) end),
    ?trans(fun() -> mnesia2:write(#test_table{i=2}) end),
    
    %% Write in test table 2
    ?trans(fun() -> mnesia2:write(#test_table2{i=3}) end),
    ?trans(fun() -> mnesia2:write(#test_table2{i=4}) end),
    mnesia2_test_lib:sync_tables(Nodes, [test_table, test_table2]),
    
    File = whow,
    %% Now make a backup
    ?match(ok, mnesia2:backup(File)),

    ?match(ok, mnesia2:install_fallback(File)),
    
    %% Now add things
    ?trans(fun() -> mnesia2:write(#test_table{i=2.5}) end),
    ?trans(fun() -> mnesia2:write(#test_table2{i=3.5}) end),

    mnesia2_test_lib:kill_mnesia2(Nodes),
    receive after 2000 -> ok end,
    ?match([], mnesia2_test_lib:start_mnesia2(Nodes, [test_table, test_table2])),

    %% Now check newly started tables
    ?match({atomic, [1,2]},
	   mnesia2:transaction(fun() -> lists:sort(mnesia2:all_keys(test_table)) end)),
    ?match({atomic, [3,4]},
	   mnesia2:transaction(fun() -> lists:sort(mnesia2:all_keys(test_table2)) end)),

    %% Test some error cases
    mnesia2:set_debug_level(debug),
    ?match({error, _}, mnesia2:install_fallback("NonExisting.FILE")),
    ?match({error, _}, mnesia2:install_fallback(filename:join(mnesia2_lib:dir(), "LATEST.LOG"))),

    %% Cleanup
    file:delete(File),
    ?verify_mnesia2(Nodes, []),
    ?cleanup(1, Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
debug(doc) ->
    ["Try out the four debug levels and ensure that the",
    "expected events are generated."];
debug(suite) -> [];
debug(Config) when is_list(Config) ->
    Nodes = ?init(1, Config),
    case application:get_env(mnesia2,debug) of
	undefined ->
	    ?match(none, mnesia2:system_info(debug));
	{ok, false} ->
	    ?match(none, mnesia2:system_info(debug));
	{ok, true} ->
	    ?match(debug, mnesia2:system_info(debug));
	{ok, Env} ->
	    ?match(Env, mnesia2:system_info(debug))
    end,

    ?match(ok, mnesia2:start([{debug, verbose}])),
    ?match(verbose, mnesia2:system_info(debug)),
    mnesia2_test_lib:kill_mnesia2(Nodes),
    receive after 2000 -> ok end,

    ?match(ok, mnesia2:start([{debug, debug}])),
    ?match(debug, mnesia2:system_info(debug)),
    mnesia2_test_lib:kill_mnesia2(Nodes),
    receive after 2000 -> ok end,

    ?match(ok, mnesia2:start([{debug, trace}])),
    ?match(trace, mnesia2:system_info(debug)),
    mnesia2_test_lib:kill_mnesia2(Nodes),
    receive after 2000 -> ok end,

    ?match(ok, mnesia2:start([{debug, true}])),
    ?match(debug, mnesia2:system_info(debug)),
    mnesia2_test_lib:kill_mnesia2(Nodes),
    receive after 2000 -> ok end,

    ?match(ok, mnesia2:start([{debug, false}])),
    ?match(none, mnesia2:system_info(debug)),

    ?verify_mnesia2(Nodes, []),
    ?cleanup(1, Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
dir(doc) ->
    ["Try to use alternate Mnesia2 directories"];
dir(suite) -> [];
dir(Config) when is_list(Config) ->
    Nodes = ?init(1, Config),

    ?match(ok, mnesia2:start([{dir, tuff}])),
    Dir = filename:join([element(2, file:get_cwd()), "tuff"]),
    ?match(Dir, mnesia2:system_info(directory)),
    mnesia2_test_lib:kill_mnesia2(Nodes),
 
    ?cleanup(1, Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
dump_log_update_in_place(doc) ->
    ["Change the update in place policy for the transaction log dumper."];
dump_log_update_in_place(suite) -> [];
dump_log_update_in_place(Config) when is_list(Config) ->
    Nodes = ?acquire(1, Config),
    ?match(true, mnesia2:system_info(dump_log_update_in_place)),
    ?match({atomic,ok},
	   mnesia2:create_table(test_table,
			       [{disc_copies, Nodes},
				{attributes,
				 record_info(fields,test_table)}])),
    
    mnesia2_test_lib:kill_mnesia2(Nodes),
    receive after 2000 -> ok end,
    
    ?match(ok, mnesia2:start([{dump_log_update_in_place, false}])),
    ?match(false, mnesia2:system_info(dump_log_update_in_place)),

    mnesia2_test_lib:sync_tables(Nodes, [schema, test_table]),

    %% Now provoke some log dumps

    L = lists:map(
	  fun(Num) ->
		  %% Write something on one end ...
		  mnesia2:transaction(
		    fun() ->
			    mnesia2:write(#test_table{i=Num}) end
		   ) end,
	  lists:seq(1, 110)),
    
    L2 = lists:duplicate(110, {atomic, ok}),

    %% If this fails then some of the 110 writes above failed
    ?match(true, L==L2),
    if  L==L2 -> ok;
	true -> 
	    ?verbose("***** List1 len: ~p, List2 len: ~p~n",
		      [length(L), length(L2)]),
	    ?verbose("L: ~p~nL2:~p~n", [L, L2])
    end,
    
    %% If we still can write, then Mnesia2 is probably alive
    ?trans(fun() -> mnesia2:write(#test_table{i=115}) end),
    
    ?verify_mnesia2(Nodes, []),
    ?cleanup(1, Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
dump_log_write_threshold(doc)->
    ["This test case must be rewritten.",
     "Dump logs are tested by doing transactions, then killing Mnesia2 and ",
     "then examining the table data files and see if they are correct.",
     "The test_table is used as a counter, test_table. is stepped once ",
     "for each transaction."];
dump_log_write_threshold(suite)->[];
dump_log_write_threshold(Config) when is_list(Config) ->
    [N1] = ?acquire_schema(1, Config),

    Threshold = 3,
    ?match(ok,mnesia2:start([{dump_log_write_threshold, Threshold}])),

    ?match({atomic,ok},
	   mnesia2:create_table(test_table,
			       [{disc_copies, [N1]},
				{attributes,
				 record_info(fields,test_table)}])),
    ?match(dumped, mnesia2:dump_log()),
    
    ?match(ok, do_trans(2)),				% Shall not have dumped
    check_logs(0),
    
    ?match(ok, do_trans(Threshold - 2)),			% Trigger a dump
    receive after 1000 -> ok end,
    check_logs(Threshold),

    
    ?match(ok, do_trans(Threshold - 1)),   
    ?match(dumped, mnesia2:dump_log()),   %% This should trigger ets2dcd dump
    check_logs(0),                       %% and leave no dcl file
    
    ?match(stopped, mnesia2:stop()),

    %% Check bad threshold value
    ?match({error,{bad_type,dump_log_write_threshold,0}},
	   mnesia2:start([{dump_log_write_threshold,0}])),

    ?verify_mnesia2([], [N1]),
    ?cleanup(1, Config),
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
dump_log_time_threshold(doc)->
    ["See doc on above."];
dump_log_time_threshold(suite)->[];
dump_log_time_threshold(Config) when is_list(Config) ->
    Nodes = ?acquire_schema(1, Config),
    Time = 4000,

    %% Check bad threshold value
    ?match({error,{bad_type,dump_log_time_threshold,0}},
	   mnesia2:start([{dump_log_time_threshold,0}])),
    
    
    ?match(ok,mnesia2:start([{dump_log_write_threshold,100},
			    {dump_log_time_threshold, Time}])),
    
    ?match({atomic,ok},mnesia2:create_table(test_table,
					   [{disc_copies, Nodes},
					    {attributes,
					     record_info(fields,
							 test_table)}])),
    
    %% Check that nothing is dumped when within time threshold
    ?match(ok, do_trans(1)),
    check_logs(0),
    
    ?match(Time, mnesia2:system_info(dump_log_time_threshold)),

    %% Check that things get dumped when time threshold exceeded
    ?match(ok, do_trans(5)),
    receive after Time+2000 -> ok end,
    check_logs(6),
    
    ?verify_mnesia2([node()], []),
    ?cleanup(1, Config),
    ok.

%%%%%%%%
%% 
%% Help functions for dump log

%% Do a transaction N times
do_trans(0) -> ok;
do_trans(N) ->
    Fun = fun() ->
		  XX=incr(),
		  mnesia2:write(#test_table{i=XX})
	  end,
    {atomic, ok} = mnesia2:transaction(Fun),
    do_trans(N-1).

%% An increasing number
incr() ->
    case get(bloody_counter) of
	undefined -> put(bloody_counter, 2), 1;
	Num -> put(bloody_counter, Num+1)
    end.

%%
%% Check that the correct number of transactions have been recorded.
%%-record(test_table,{i,a1,a2,a3}).
check_logs(N) ->    
    File = mnesia2_lib:tab2dcl(test_table),    
    Args = [{file, File}, {name, testing}, {repair, true}, {mode, read_only}],

    if N == 0 ->
	    ?match(false, mnesia2_lib:exists(File));
       true ->
	    ?match(true, mnesia2_lib:exists(File)),
	    ?match({ok, _Log}, disk_log:open(Args)),
	    
	    {Cont, Terms} = disk_log:chunk(testing, start),
	    ?match(eof, disk_log:chunk(testing, Cont)),
	    %%?verbose("N: ~p, L: ~p~n", [N, L]),
	    disk_log:close(testing),
	    
	    %% Correct number of records in file
	    ?match({N, N}, {N, length(Terms) -1 })  %% Ignore Header
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

dump_log_load_regulation(doc) ->
    ["Test the load regulation of the dumper"];
dump_log_load_regulation(suite) ->
    [];
dump_log_load_regulation(Config) when is_list(Config) ->
    Nodes = ?acquire_nodes(1, Config),
    Param = dump_log_load_regulation,

    %% Normal 
    NoReg = false,
    ?match(NoReg, mnesia2:system_info(Param)),
    ?match([], mnesia2_test_lib:stop_mnesia2(Nodes)),

    %% Bad
    Bad = arne_anka,
    ?match({error, {bad_type, Param, Bad}},
	   mnesia2:start([{Param, Bad}])),

    %% Regulation activated
    Reg = true,
    ?match(ok,mnesia2:start([{Param, Reg}])),
    ?match(Reg, mnesia2:system_info(Param)),

    Args =
	[{db_nodes, Nodes},
	 {driver_nodes, Nodes},
	 {replica_nodes, Nodes},
	 {n_drivers_per_node, 5},
	 {n_branches, length(Nodes) * 10},
	 {n_accounts_per_branch, 5},
	 {replica_type, disc_copies},
	 {stop_after, timer:seconds(15)},
	 {report_interval, timer:seconds(3)},
	 {use_running_mnesia2, true},
	 {reuse_history_id, true}],
    
    ?match({ok, _}, mnesia2_tpcb:start(Args)),
    
    ?verify_mnesia2(Nodes, []),
    ?cleanup(1, Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

max_wait_for_decision(doc) ->
    ["Provoke Mnesia2 to make a forced decision of the outome",
     "of a heavy weight transaction."];
max_wait_for_decision(suite) -> [];
max_wait_for_decision(_Config) -> ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

send_compressed(doc) -> [];
send_compressed(suite) -> [];
send_compressed(Config) ->
    [N1,N2] = Nodes = ?acquire_nodes(2, Config),
    ?match({atomic,ok}, mnesia2:create_table(t0, [{ram_copies,[N1,N2]}])),
    ?match({atomic,ok}, mnesia2:create_table(t1, [{disc_copies,[N1,N2]}])),
    ?match({atomic,ok}, mnesia2:create_table(t2, [{disc_only_copies,[N1,N2]}])),

    Max = 1000,
    Create = fun(Tab) -> [mnesia2:write({Tab, N, {N, "FILLER-123490878345asdasd"}})
			  || N <- lists:seq(1, Max)],
			 ok
	     end,
    
    ?match([], mnesia2_test_lib:kill_mnesia2([N2])),
    sys:get_status(mnesia2_monitor), %% sync N1
    ?match([], mnesia2_test_lib:kill_mnesia2([N1])),
    ?match(ok, mnesia2:start([{send_compressed, 9}])),
    ?match(ok, mnesia2:wait_for_tables([t0,t1,t2], 25000)),

    ?match({atomic, ok}, mnesia2:transaction(Create, [t0])),
    ?match({atomic, ok}, mnesia2:transaction(Create, [t1])),
    ?match({atomic, ok}, mnesia2:transaction(Create, [t2])),
    
    ?match([], mnesia2_test_lib:start_mnesia2([N2], [t0,t1,t2])),
    
    Verify = fun(Tab) ->		     
		     [ [{Tab,N,{N,_}}] = mnesia2:read(Tab, N) || N <- lists:seq(1, Max)],
		     ok
	     end,
    ?match({atomic, ok}, rpc:call(N1, mnesia2, transaction, [Verify, [t0]])),
    ?match({atomic, ok}, rpc:call(N1, mnesia2, transaction, [Verify, [t1]])),
    ?match({atomic, ok}, rpc:call(N1, mnesia2, transaction, [Verify, [t2]])),
    
    ?match({atomic, ok}, rpc:call(N2, mnesia2, transaction, [Verify, [t0]])),
    ?match({atomic, ok}, rpc:call(N2, mnesia2, transaction, [Verify, [t1]])),
    ?match({atomic, ok}, rpc:call(N2, mnesia2, transaction, [Verify, [t2]])),
    
    ?verify_mnesia2(Nodes, []),
    ?cleanup(1, Config),
    ok.

app_test(doc) -> [];
app_test(suite) -> [];
app_test(_Config) ->
    ?match(ok,test_server:app_test(mnesia2)),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

event_module(doc) ->
    ["Replace the event module with another module and use it as",
     "receiver of the various system and table events. Provoke",
     "coverage of all kinds of events."];
event_module(suite) -> [];
event_module(Config) when is_list(Config) ->
    Filter = fun({mnesia2_system_event,{mnesia2_info, _, _}}) -> false;
		(_) -> true
	     end,
    
    [_N1, N2]=Nodes=?acquire_schema(2, Config),

    Def = case mnesia2_test_lib:diskless(Config) of 
	      true -> [{event_module, mnesia2_config_event},
		       {extra_db_nodes, Nodes}];
	      false  ->
		  [{event_module, mnesia2_config_event}]
	  end,

    ?match({[ok, ok], []}, rpc:multicall(Nodes, mnesia2, start, [Def])),
    receive after 1000 -> ok end,
    mnesia2_event ! {get_log, self()},
    DebugLog1 = receive 
		    {log, L1} -> L1
		after 10000 -> [timeout]
		end,
    ?match([{mnesia2_system_event,{mnesia2_up,N2}}],
	   lists:filter(Filter, DebugLog1)),
    mnesia2_test_lib:kill_mnesia2([N2]),
    receive after 2000 -> ok end,

    ?match({[ok], []}, rpc:multicall([N2], mnesia2, start, [])),

    receive after 1000 -> ok end,
    mnesia2_event ! {get_log, self()},
    DebugLog = receive 
		   {log, L} -> L
	       after 10000 -> [timeout]
	       end,
    ?match([{mnesia2_system_event,{mnesia2_up,N2}},
	    {mnesia2_system_event,{mnesia2_down,N2}},
	    {mnesia2_system_event,{mnesia2_up, N2}}],
	   lists:filter(Filter, DebugLog)),
    ?verify_mnesia2(Nodes, []),
    ?cleanup(1, Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_one_disc_full_then_one_disc_less(doc)->
    ["Start a disk node and then a disk less one. Distribute some",
     "tables between them."];
start_one_disc_full_then_one_disc_less(suite) -> [];
start_one_disc_full_then_one_disc_less(Config) when is_list(Config) ->
    [N1, N2] = ?init(2, Config),
    ?match(ok, mnesia2:create_schema([N1])),
    ?match([], mnesia2_test_lib:start_mnesia2([N1])),

    ?match({atomic, ok}, mnesia2:add_table_copy(schema, N2, ram_copies)),

    ?match(ok, rpc:call(N2, mnesia2, start, [[{schema_location, ram},
					     {extra_db_nodes, [N1]}]])),
    mnesia2_test_lib:sync_tables([N1, N2], [schema]),

    %% Now create some tables
    ?match({atomic,ok},
	   mnesia2:create_table(test_table,
			       [{ram_copies, [N1, N2]},
				{attributes,
				 record_info(fields,test_table)}])),
    
    ?match({atomic,ok},
	   rpc:call(
	     N2, mnesia2,create_table, [test_table2,
				      [{ram_copies, [N1, N2]},
				       {attributes,
					record_info(fields,test_table2)}]])),

    %% Write something on one end ...
    Rec = #test_table{i=55},
    ?match({atomic, ok},
	   mnesia2:transaction(fun() -> mnesia2:write(Rec) end)),
    
    %% ... and read it in the other
    ?match({atomic, [Rec]},
	   rpc:call(N2, mnesia2, transaction, 
		    [fun() -> mnesia2:read({test_table, 55}) end])),
    
    
    %% Then do the same but start at the other end
    Rec2 = #test_table2{i=155},
    ?match({atomic, ok},
	   rpc:call(N2, mnesia2, transaction, 
		    [fun() ->
			     mnesia2:write(Rec2) end
		    ])),
    
    ?match({atomic, [Rec2]},
	   mnesia2:transaction(fun() -> mnesia2:read({test_table2, 155}) end)),
    
    ?verify_mnesia2([N1, N2], []),
    ?cleanup(2, Config),
    ok.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_first_one_disc_less_then_one_disc_full(doc)->
    ["no_doc"];
start_first_one_disc_less_then_one_disc_full(suite) -> [];
start_first_one_disc_less_then_one_disc_full(Config) when is_list(Config) ->
    [N1, N2] = Nodes = ?init(2, Config),
    ?match(ok, mnesia2:create_schema([N1])),
    ?match([], mnesia2_test_lib:start_mnesia2([N1])),

    ?match({atomic, ok}, mnesia2:add_table_copy(schema, N2, ram_copies)),
    
    ?match(ok, rpc:call(N2, mnesia2, start, [[{schema_location, ram},
					     {extra_db_nodes, Nodes}]])),

    mnesia2_test_lib:sync_tables([N1, N2], [schema]),
    
    mnesia2_test_lib:kill_mnesia2(Nodes),
    receive after 2000 -> ok end,
    ?match([], mnesia2_test_lib:start_mnesia2(Nodes)),

    mnesia2_test_lib:sync_tables([N1, N2], [schema]),
    
    %% Now create some tables
    ?match({atomic,ok},
	   rpc:call(
	     N1, mnesia2,create_table, [test_table,
				       [%%{disc_copies, [node()]},
					{ram_copies, [N1, N2]},
					{attributes,
					 record_info(fields,test_table)}]])),
    mnesia2_test_lib:sync_tables([N1, N2], [test_table]),

    ?match({atomic,ok},
	   rpc:call(
	     N2, mnesia2,create_table, [test_table2,
				       [%%{disc_copies, [node()]},
					{ram_copies, [N1, N2]},
					{attributes,
					 record_info(fields,test_table2)}]])),
    
    mnesia2_test_lib:sync_tables([N1, N2], [test_table, test_table2]),

    %% Assure tables loaded
    ?match({[ok, ok], []},
	   rpc:multicall([N1, N2], mnesia2, wait_for_tables,
			 [[schema, test_table, test_table2], 10000])),
    
    %% Write something on one end ...
    Rec = #test_table{i=55},
    ?match({atomic, ok},
	   rpc:call(N1, mnesia2, transaction, 
		    [fun() -> mnesia2:write(Rec) end])),
    
    %% ... and read it in the other
    ?match({atomic, [Rec]},
	   rpc:call(N2, mnesia2, transaction, 
		    [fun() -> mnesia2:read({test_table, 55}) end])),
    
    %% Then do the same but start at the other end
    Rec2 = #test_table2{i=155},
    ?match({atomic, ok},
	   rpc:call(N2, mnesia2, transaction, 
		    [fun() ->
			     mnesia2:write(Rec2) end
		    ])),
    
    ?match({atomic, [Rec2]},
	   rpc:call(N1, mnesia2, transaction, 
		    [fun() -> mnesia2:read({test_table2, 155}) end])),
    
    ?verify_mnesia2(Nodes, []),
    ?cleanup(1, Config),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
start_first_one_disc_less_then_two_more_disc_less(doc)->
    ["no doc"];
start_first_one_disc_less_then_two_more_disc_less(suite) -> [];
start_first_one_disc_less_then_two_more_disc_less(Config) when is_list(Config) ->
    Nodes = [N1, N2, N3] = ?init(3, Config),

    ?match(ok, rpc:call(N1, mnesia2, start, [[{schema_location, ram}]])),

    %% Really should use test_lib:mnesia2_start for these ones but ...
    ?match({atomic, ok}, 
	   rpc:call(N1, mnesia2,add_table_copy, [schema, N2, ram_copies])),
    ?match({atomic, ok}, 
	   rpc:call(N1, mnesia2,add_table_copy, [schema, N3, ram_copies])),
    
    ?match(ok, rpc:call(N2, mnesia2, start, [[{schema_location, ram},
					     {extra_db_nodes, [N1]}]])),
    ?match(ok, rpc:call(N3, mnesia2, start, [[{schema_location, ram},
					     {extra_db_nodes, [N1, N2]}]])),

    %% Now create some tables
    ?match({atomic,ok},
	   rpc:call(
	     N1, mnesia2,create_table, [test_table,
				      [%%{disc_copies, [node()]},
				       {ram_copies, [N1, N2, N3]},
				       {attributes,
					record_info(fields,test_table)}]])),

    %% Assure tables loaded
    ?match({[ok, ok, ok], []},
	   rpc:multicall([N1, N2, N3], mnesia2, wait_for_tables,
			 [[test_table], 1000])),

    %% Write something on one end ...
    ?match({atomic, ok},
	   rpc:call(N1, mnesia2, transaction, 
		    [fun() -> mnesia2:write(#test_table{i=44}) end])),

    %% Force synchronicity
    ?match({atomic, ok},
	   rpc:call(N1, mnesia2, transaction, 
		    [fun() -> mnesia2:write_lock_table(test_table) end])),
    
    %% ... and read it in the others
    ?match({[{atomic, [{test_table, 44, _, _, _}]},
	     {atomic, [{test_table, 44, _, _, _}]}], []},
	   rpc:multicall([N2, N3], mnesia2, transaction, 
			 [fun() -> mnesia2:read({test_table, 44}) end])),
    
    %% Then do the other way around
    ?match({atomic, ok},
	   rpc:call(N3, mnesia2, transaction, 
		    [fun() -> mnesia2:write(#test_table{i=33}) end])),
    %% Force synchronicity
    ?match({atomic, ok},
	   rpc:call(N3, mnesia2, transaction, 
		    [fun() -> mnesia2:write_lock_table(test_table) end])),
    
    ?match({[{atomic, [{test_table, 44, _, _, _}]},
	     {atomic, [{test_table, 44, _, _, _}]}], []},
	   rpc:multicall([N1, N2], mnesia2, transaction, 
			 [fun() -> mnesia2:read({test_table, 44}) end])),

    mnesia2_test_lib:reload_appls([mnesia2], Nodes),
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
schema_location_and_extra_db_nodes_combinations(doc)->
    ["Test schema loaction and extra_db_nodes combinations."];
schema_location_and_extra_db_nodes_combinations(suite) -> [];
schema_location_and_extra_db_nodes_combinations(Config) when is_list(Config) ->
    [N1, N2] = Nodes = ?init(2, Config),
    ?match(ok, mnesia2:create_schema([N1])),
    ?match([], mnesia2_test_lib:start_mnesia2([N1])),
    
    %% Really should use test_lib:mnesia2_start for these ones but ...
    ?match({atomic, ok}, 
	   rpc:call(N1, mnesia2,add_table_copy, [schema, N2, ram_copies])),
    
    ?match(ok, rpc:call(N2, mnesia2, start, [[{schema_location, ram},
					     {extra_db_nodes, [N1]}]])),
    
    %% Assure tables loaded
    ?match({[ok, ok], []},
	   rpc:multicall([N1, N2], mnesia2, wait_for_tables,
			 [[schema], 10000])),
    
    ?verify_mnesia2(Nodes, []),
    ?cleanup(2, Config),
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
table_load_to_disc_less_nodes(doc)->
    ["Load tables to disc less nodes"];
table_load_to_disc_less_nodes(suite) -> [];
table_load_to_disc_less_nodes(Config) when is_list(Config) ->
    [N1, N2] = ?init(2, Config),

    ?match(ok, rpc:call(N1, mnesia2, start, [[{schema_location, ram}]])),

    %% Really should use test_lib:mnesia2_start for these ones but ...
    ?match({atomic, ok}, 
	   rpc:call(N1, mnesia2,add_table_copy, [schema, N2, ram_copies])),
    
    ?match(ok, rpc:call(N2, mnesia2, start, [[{schema_location, ram},
					     {extra_db_nodes, [N1]}]])),

    %% Now create some tables
    ?match({atomic,ok},
	   rpc:call(
	     N1, mnesia2,create_table, [test_table,
				      [%%{disc_copies, [node()]},
				       {ram_copies, [N1, N2]},
				       {attributes,
					record_info(fields,test_table)}]])),

    %% Assure tables loaded
    ?match({[ok, ok], []},
	   rpc:multicall([N1, N2], mnesia2, wait_for_tables,
			 [[test_table], 1000])),

    %% Write something on one end ...
    ?match({atomic, ok},
	   rpc:call(N1, mnesia2, transaction, 
		    [fun() -> mnesia2:write(#test_table{i=44}) end])),

    %% Force synchronicity
    ?match({atomic, ok},
	   rpc:call(N1, mnesia2, transaction, 
		    [fun() -> mnesia2:write_lock_table(test_table) end])),
    
    %% ... and read it in the others
    ?match({atomic, [{test_table, 44, _, _, _}]},
	   rpc:call(N2, mnesia2, transaction, 
		    [fun() -> mnesia2:read({test_table, 44}) end])),

    ?cleanup(2, Config),
    ok.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
schema_merge(doc) ->
    ["Provoke various schema merge situations.",
     "Perform various schema updates while some nodes are down,",
     "stop the started nodes, start the stopped nodes and perform",
     "schema updates. Now we have a situation were some of the table",
     "definitions have been changed on two or more nodes independently",
     "of each other and when mnesia2 on the nodes tries to connect",
     "to each other at restart the schema will be merged.",
     "Do also try to provoke schema merge situations were the",
     "schema cannot be merged."];

schema_merge(suite) -> [];

schema_merge(Config) when is_list(Config) ->
    [N1, N2]=Nodes=?acquire(2,Config),
    
    mnesia2_test_lib:kill_mnesia2([N2]),    
    receive after 1000 -> ok end,

    Storage = mnesia2_test_lib:storage_type(disc_copies, Config),    
    ?match({atomic,ok},
	   rpc:call(
	     N1, mnesia2,create_table, 
	     [test_table,
	      [{Storage, [N1]},
	       {attributes,
		record_info(fields,test_table)}]])),
    
    ?match({atomic, ok},
	   rpc:call(N1, mnesia2, transaction, 
		    [fun() -> mnesia2:write(#test_table{i=44}) end])),

    mnesia2_test_lib:kill_mnesia2([N1]),
    receive after 2000 -> ok end,
    %% Can't use std start because it waits for schema
    ?match(ok, rpc:call(N2, mnesia2, start, [])),

    ?match({atomic,ok},
	   rpc:call(
	     N2, mnesia2,create_table, 
	     [test_table2,
	      [{Storage, [N2]},
	       {attributes,
		record_info(fields,test_table2)}]])),
    
    receive after 5000 -> ok end,

    ?match({atomic, ok},
	   rpc:call(N2, mnesia2, transaction, 
		    [fun() -> mnesia2:write(#test_table2{i=33}) end])),
    
    %% Can't use std start because it waits for schema
    ?match(ok, rpc:call(N1, mnesia2, start, [])),

    %% Assure tables loaded
    ?match({[ok, ok], []},
	   rpc:multicall([N1, N2], mnesia2, wait_for_tables,
			 [[schema, test_table, test_table2], 10000])),
    
    %% ... and read it in the others
    ?match({[{atomic, [{test_table, 44, _, _, _}]},
	     {atomic, [{test_table, 44, _, _, _}]}], []},
	   rpc:multicall([N1, N2], mnesia2, transaction, 
			 [fun() -> mnesia2:read({test_table, 44}) end])),
    
    ?match({[{atomic, [{test_table2, 33, _}]},
	     {atomic, [{test_table2, 33, _}]}], []},
	   rpc:multicall([N1, N2], mnesia2, transaction, 
			 [fun() -> mnesia2:read({test_table2, 33}) end])),
    
    ?verify_mnesia2(Nodes, []),
    ?cleanup(2, Config),
    ok.


-define(connect(Nodes), mnesia2:change_config(extra_db_nodes, Nodes)).
-define(rpc_connect(From, Nodes), 
	rpc:call(From, mnesia2, change_config, [extra_db_nodes, Nodes])).


sort({ok, NS}) ->
    {ok, lists:sort(NS)};
sort(Ns) when is_tuple(Ns) ->
    Ns;
sort(NS) when is_list(NS) -> 
    lists:sort(NS).




dynamic_basic(suite) -> [];
dynamic_basic(Config) when is_list(Config) ->
    Nodes = [N1, N2, N3] = ?acquire_nodes(3, Config),
    SNs = lists:sort(Nodes),    

    ?match({atomic, ok}, mnesia2:create_table(tab1, [{ram_copies, Nodes--[N1]}, {disc_copies, [N1]}])),
    ?match({atomic, ok}, mnesia2:create_table(tab2, [{disc_copies, Nodes}])),

    ?match({ok, SNs}, sort(?rpc_connect(N1, Nodes))),     %% What shall happen?
    ?match({ok, []}, sort(?rpc_connect(N1, [nonode@nothosted]))),  %% What shall happen?
    
    ?match([], mnesia2_test_lib:kill_mnesia2([N2])),
    ?match(ok, mnesia2:delete_schema([N2])),

    ?match(ok, mnesia2:dirty_write({tab1, 1, 1})),
    ?match(ok, mnesia2:dirty_write({tab2, 1, 1})),
   
    ?match(ok, rpc:call(N2, mnesia2, start, [[{extra_db_nodes, [N1]}]])),
    ?match(ok, rpc:call(N2, mnesia2, wait_for_tables, [[tab1,tab2],5000])),
    io:format("Here ~p ~n",[?LINE]), 
    check_storage(N2, N1, [N3]),
    ?match(SNs, sort(rpc:call(N1, mnesia2, system_info, [running_db_nodes]))),
    ?match(SNs, sort(rpc:call(N2, mnesia2, system_info, [running_db_nodes]))),

    ?match([], mnesia2_test_lib:kill_mnesia2([N3])),
    ?match(ok, mnesia2:delete_schema([N3])),
    
    io:format("T1 ~p ~n",[rpc:call(N3,?MODULE,c_nodes,[])]),
    ?match(ok, rpc:call(N3, mnesia2, start, [])),
    io:format("T2 ~p ~n",[rpc:call(N3,?MODULE,c_nodes,[])]),
    timer:sleep(2000),
    io:format("T3 ~p ~n",[rpc:call(N3,?MODULE,c_nodes,[])]),
    ?match({ok, [N1]}, sort(?rpc_connect(N3, [N1]))),
    io:format("T4 ~p ~n",[rpc:call(N3,?MODULE,c_nodes,[])]),
    ?match(ok, rpc:call(N3, mnesia2, wait_for_tables, [[tab1,tab2],5000])),
    io:format("Here ~p ~n",[?LINE]), 
    check_storage(N3, N1, [N2]),
    ?match(SNs, sort(rpc:call(N1, mnesia2, system_info, [running_db_nodes]))),
    ?match(SNs, sort(rpc:call(N2, mnesia2, system_info, [running_db_nodes]))),
    
    ?match([], mnesia2_test_lib:kill_mnesia2([N3])),
    ?match(ok, mnesia2:delete_schema([N3])),
    
    ?match(ok, rpc:call(N3, mnesia2, start, [])),
    ?match({ok, [N3]}, sort(?rpc_connect(N1, [N3]))),
    ?match(ok, rpc:call(N3, mnesia2, wait_for_tables, [[tab1,tab2],5000])),
    io:format("Here ~p ~n",[?LINE]), 
    check_storage(N3, N1, [N2]),
    ?match(SNs, sort(rpc:call(N1, mnesia2, system_info, [running_db_nodes]))),
    ?match(SNs, sort(rpc:call(N2, mnesia2, system_info, [running_db_nodes]))),

    mnesia2_test_lib:kill_mnesia2([N2]),
    ?match(ok, mnesia2:delete_schema([N2])),
    ?match({atomic, ok}, mnesia2:del_table_copy(schema, N2)),

    % Ok, we have now removed references to node N2 from the other nodes
    % mnesia2 should come up now.
    ?match({atomic, ok}, mnesia2:add_table_copy(tab1, N2, ram_copies)),

    ?match(ok, rpc:call(N2, mnesia2, start, [])),
    ?match({ok, _}, sort(?rpc_connect(N2, [N3]))),
    
    ?match(SNs, sort(rpc:call(N1, mnesia2, system_info, [running_db_nodes]))),
    ?match(SNs, sort(rpc:call(N2, mnesia2, system_info, [running_db_nodes]))),
    ?match(SNs, sort(rpc:call(N3, mnesia2, system_info, [running_db_nodes]))),

    ?match(ok, rpc:call(N2, mnesia2, wait_for_tables, [[tab1], 1000])),
    ?match([{tab1, 1, 1}], rpc:call(N2, mnesia2, dirty_read, [tab1, 1])),
       
    mnesia2_test_lib:kill_mnesia2([N2]),

    %%% SYNC!!!
    timer:sleep(1000),
    sys:get_status(mnesia2_monitor),

    ?match([N3,N1], sort(rpc:call(N1, mnesia2, system_info, [running_db_nodes]))),
    ?match([N3,N1], sort(rpc:call(N3, mnesia2, system_info, [running_db_nodes]))),
    
    ?match(ok, rpc:call(N2, mnesia2, start, [])),
    ?match({ok, _}, sort(?rpc_connect(N3, [N2]))),
    
    ?match(SNs, sort(rpc:call(N1, mnesia2, system_info, [running_db_nodes]))),
    ?match(SNs, sort(rpc:call(N2, mnesia2, system_info, [running_db_nodes]))),
    ?match(SNs, sort(rpc:call(N3, mnesia2, system_info, [running_db_nodes]))),

    ?match(ok, rpc:call(N2, mnesia2, wait_for_tables, [[tab1], 1000])),
    ?match([{tab1, 1, 1}], rpc:call(N2, mnesia2, dirty_read, [tab1, 1])),

    ?verify_mnesia2(Nodes, []),
%%    ?cleanup(3, Config).
    ok.

c_nodes() -> 		  						
    {mnesia2_lib:val({current, db_nodes}),mnesia2_lib:val(recover_nodes)}.


dynamic_ext(suite) ->    [];
dynamic_ext(Config) when is_list(Config) ->
    Ns = [N1,N2] = ?acquire_nodes(2, Config),
    SNs = lists:sort([N1,N2]),
    
    ?match({atomic, ok}, mnesia2:create_table(tab0, [{disc_copies, [N1,N2]}])),
    ?match({atomic, ok}, mnesia2:create_table(tab1, [{ram_copies, [N2]}])),
    ?match({atomic, ok}, mnesia2:create_table(tab2, [{disc_copies, [N2]}])),
    ?match({atomic, ok}, mnesia2:create_table(tab3, [{disc_only_copies, [N2]}])),
    
    mnesia2_test_lib:kill_mnesia2([N2]),
    ?match(ok, mnesia2:delete_schema([N2])),
    ?match(ok, rpc:call(N2, mnesia2, start, [[{extra_db_nodes, [N1]}]])),
    
    ?match(SNs, sort(rpc:call(N1, mnesia2, system_info, [running_db_nodes]))),
    ?match(SNs, sort(rpc:call(N2, mnesia2, system_info, [running_db_nodes]))),
    
    ?match(ok, rpc:call(N2, mnesia2, wait_for_tables, [[tab0,tab1,tab2,tab3], 2000])),

    Check = fun({Tab,Storage}) ->
		    ?match(Storage, rpc:call(N2, mnesia2, table_info, [Tab, storage_type])),
		    ?match([{N2,Storage}], 
			   lists:sort(rpc:call(N2, mnesia2, table_info, [Tab, where_to_commit])))
	    end,
    [Check(Test) || Test <- [{tab1, ram_copies},{tab2, disc_copies},{tab3, disc_only_copies}]],
    
    T = erlang:unique_integer(),
    ?match(ok, mnesia2:dirty_write({tab0, 42, T})),
    ?match(ok, mnesia2:dirty_write({tab1, 42, T})),
    ?match(ok, mnesia2:dirty_write({tab2, 42, T})),
    ?match(ok, mnesia2:dirty_write({tab3, 42, T})),
    
    ?match(stopped, rpc:call(N2, mnesia2, stop, [])),
    ?match(ok, rpc:call(N2, mnesia2, start, [])),
    ?match(SNs, sort(rpc:call(N2, mnesia2, system_info, [running_db_nodes]))),
    ?match(ok, mnesia2:wait_for_tables([tab0,tab1,tab2,tab3], 10000)),
    ?match(ok, rpc:call(N2, mnesia2, wait_for_tables, [[tab1,tab2,tab3], 100])),
    ?match([], mnesia2:dirty_read({tab1, 41})),
    ?match([{tab2,42,T}], mnesia2:dirty_read({tab2, 42})),
    ?match([{tab3,42,T}], mnesia2:dirty_read({tab3, 42})),

    mnesia2_test_lib:kill_mnesia2([N2]),
    ?match(ok, mnesia2:delete_schema([N2])),

    ?match(stopped, rpc:call(N1, mnesia2, stop, [])),

    ?match(ok, rpc:call(N2, mnesia2, start, [[{extra_db_nodes,[N1,N2]}]])),
    ?match({timeout,[tab0]}, rpc:call(N2, mnesia2, wait_for_tables, [[tab0], 500])),

    ?match(ok, rpc:call(N1, mnesia2, start, [[{extra_db_nodes, [N1,N2]}]])),
    ?match(ok, rpc:call(N1, mnesia2, wait_for_tables, [[tab0], 1500])),
    ?match(ok, rpc:call(N2, mnesia2, wait_for_tables, [[tab0], 1500])),
    ?match([{tab0,42,T}], mnesia2:dirty_read({tab0, 42})),
    ?match([{tab0,42,T}], rpc:call(N2, mnesia2,dirty_read,[{tab0,42}])),

    ?match(stopped, rpc:call(N1, mnesia2, stop, [])),
    mnesia2_test_lib:kill_mnesia2([N2]),
    ?match(ok, mnesia2:delete_schema([N2])),
    ?match(ok, rpc:call(N1, mnesia2, start, [[{extra_db_nodes, [N1,N2]}]])),   
    ?match({timeout,[tab0]}, rpc:call(N1, mnesia2, wait_for_tables, [[tab0], 500])),

    ?match(ok, rpc:call(N2, mnesia2, start, [[{extra_db_nodes,[N1,N2]}]])),
    ?match(ok, rpc:call(N1, mnesia2, wait_for_tables, [[tab0], 1500])),
    ?match(ok, rpc:call(N2, mnesia2, wait_for_tables, [[tab0], 1500])),
    ?match([{tab0,42,T}], mnesia2:dirty_read({tab0, 42})),
    ?match([{tab0,42,T}], rpc:call(N2,mnesia2,dirty_read,[{tab0,42}])),

    ?verify_mnesia2(Ns, []),
    ok.

check_storage(Me, Orig, Other) ->
    io:format("Nodes ~p ~p ~p~n",[Me,Orig,Other]),
    rpc:multicall(Other, sys, status, [mnesia2_locker]),
    rpc:call(Me, sys, status, [mnesia2_locker]),
    rpc:call(Orig, sys, status, [mnesia2_locker]),
    rpc:multicall(Other, sys, status, [mnesia2_controller]),
    rpc:call(Me, sys, status, [mnesia2_controller]),
    rpc:call(Orig, sys, status, [mnesia2_controller]),
    %% Verify disc_copies
    W2C = lists:sort([{Node,disc_copies} || Node <- [Me,Orig|Other]]),
    W2W = lists:sort([Me,Orig|Other]),
    ?match(disc_copies, rpc:call(Orig, mnesia2, table_info, [schema, storage_type])),
    ?match(disc_copies, rpc:call(Me, mnesia2, table_info, [schema, storage_type])),
    ?match(W2C, lists:sort(rpc:call(Orig, mnesia2, table_info, [schema, where_to_commit]))),
    ?match(W2C, lists:sort(rpc:call(Me, mnesia2, table_info, [schema, where_to_commit]))),
    
    ?match(disc_copies, rpc:call(Orig, mnesia2, table_info, [tab2, storage_type])),
    ?match(disc_copies, rpc:call(Me, mnesia2, table_info, [tab2, storage_type])),    
    ?match(W2W, lists:sort(rpc:call(Me, mnesia2, table_info, [tab2, where_to_write]))),    
    ?match(Me, rpc:call(Me, mnesia2, table_info, [tab2, where_to_read])),    
    
    ?match(W2C, lists:sort(rpc:call(Orig, mnesia2, table_info, [tab2, where_to_commit]))),
    ?match(W2C, lists:sort(rpc:call(Me, mnesia2, table_info, [tab2, where_to_commit]))),
    
    ?match([{tab1,1,1}], mnesia2:dirty_read(tab1,1)),
    ?match([{tab2,1,1}], mnesia2:dirty_read(tab2,1)),
    ?match([{tab1,1,1}], rpc:call(Me, mnesia2, dirty_read, [tab1,1])),
    ?match([{tab2,1,1}], rpc:call(Me, mnesia2, dirty_read, [tab2,1])),

    ?match(true, rpc:call(Me, mnesia2_monitor, use_dir, [])),
    ?match(disc_copies, rpc:call(Me, mnesia2_lib, val, [{schema, storage_type}])),
    
    mnesia2_test_lib:kill_mnesia2([Orig]),
    mnesia2_test_lib:kill_mnesia2(Other),
    T = erlang:unique_integer(),
    ?match(ok, rpc:call(Me, mnesia2, dirty_write, [{tab2, 42, T}])),
    ?match(stopped, rpc:call(Me, mnesia2, stop, [])),
    ?match(ok, rpc:call(Me, mnesia2, start, [])),   
    ?match([], mnesia2_test_lib:start_mnesia2([Orig|Other], [tab1,tab2])),
    ?match([{tab2,42,T}], rpc:call(Me, mnesia2, dirty_read, [{tab2, 42}])),
    ?match([{tab2,42,T}], rpc:call(Orig, mnesia2, dirty_read, [{tab2, 42}])),
    
    ?match([{tab1,1,1}], mnesia2:dirty_read(tab1,1)),
    ?match([{tab2,1,1}], mnesia2:dirty_read(tab2,1)),
    ?match([{tab1,1,1}], rpc:call(Me, mnesia2, dirty_read, [tab1,1])),
    ?match([{tab2,1,1}], rpc:call(Me, mnesia2, dirty_read, [tab2,1])),    
    ok.
    

dynamic_bad(suite) ->    [];
dynamic_bad(Config) when is_list(Config) ->
    Ns = [N1, N2, N3] = ?acquire_nodes(3, Config),
    SNs = lists:sort([N2,N3]), 

    ?match({atomic, ok}, mnesia2:change_table_copy_type(schema, N2, ram_copies)),
    ?match({atomic, ok}, mnesia2:change_table_copy_type(schema, N3, ram_copies)),
    ?match({atomic, ok}, mnesia2:create_table(tab1, [{ram_copies, Ns -- [N1]},
						    {disc_copies, [N1]}])),
    ?match(ok, mnesia2:dirty_write({tab1, 1, 1})),
    
    mnesia2_test_lib:kill_mnesia2(Ns),
    ?match({[ok, ok], []}, rpc:multicall(Ns -- [N1], mnesia2, start, [])),
    ?match({ok, [N2]}, ?rpc_connect(N3, [N2])),
    ?match(SNs, sort(rpc:call(N2, mnesia2, system_info, [running_db_nodes]))),
    ?match(SNs, sort(rpc:call(N3, mnesia2, system_info, [running_db_nodes]))),
    ?match({badrpc, {'EXIT', {aborted, {no_exists, _, _}}}},
	   rpc:call(N2, mnesia2, table_info, [tab1, where_to_read])),
    
    ?match(ok, mnesia2:start()),
    ?match(ok, rpc:call(N2, mnesia2, wait_for_tables, [[tab1], 1000])),
    ?match(N2, rpc:call(N2, mnesia2, table_info, [tab1, where_to_read])), 
    ?match([{tab1, 1, 1}], rpc:call(N2, mnesia2, dirty_read, [tab1, 1])),
    
    mnesia2_test_lib:kill_mnesia2(Ns),
    ?match({[ok, ok], []}, rpc:multicall(Ns -- [N1], mnesia2, start, [])),
    ?match({ok, [N2]}, ?rpc_connect(N3, [N2])),
    % Make a merge conflict
    ?match({atomic, ok}, rpc:call(N3, mnesia2, create_table, [tab1, []])),
    
    io:format("We expect a mnesia2 crash here~n", []),
    ?match({error,{_, _}}, mnesia2:start()),

    ?verify_mnesia2(Ns -- [N1], []),
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
unknown_config(doc) ->
    ["Try some unknown configuration parameters and see that expected",
     "things happens."];
unknown_config(suite)-> [];
unknown_config(Config) when is_list(Config) ->
    ?init(1, Config),
    %% NOTE: case 1 & 2 below do not respond the same
    ?match({error, Res} when element(1, Res) == bad_type,
	   mnesia2:start([{undefined_config,[]}])),
    %% Below does not work, but the "correct" behaviour would be to have
    %% case 1 above to behave as the one below.

    %% in mnesia2-1.3 {error,{bad_type,{[],undefined_config}}}
    ?match({error, Res} when element(1, Res) == bad_type,
	   mnesia2:start([{[],undefined_config}])),
    ?cleanup(1, Config),
    ok.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
inconsistent_database(doc) ->
    ["Replace the event module with another module and use it as",
     "receiver of the various system and table events. Provoke",
     "coverage of all kinds of events."];
inconsistent_database(suite) -> [];
inconsistent_database(Config) when is_list(Config) ->
    Nodes = mnesia2_test_lib:prepare_test_case([{init_test_case, [mnesia2]}], 
					      2, Config, ?FILE, ?LINE),
    KillAfter = length(Nodes) * timer:minutes(5),
    ?acquire_schema(2, Config ++ [{tc_timeout, KillAfter}]),

    Ok = [ok || _N <- Nodes],
    StartArgs = [{event_module, mnesia2_inconsistent_database_test}],
    ?match({Ok, []}, rpc:multicall(Nodes, mnesia2, start, [StartArgs])),
    ?match([], mnesia2_test_lib:kill_mnesia2(Nodes)),
    
    ?match(ok, mnesia2_meter:go(ram_copies, Nodes)),

    mnesia2_test_lib:reload_appls([mnesia2], Nodes),
    ok.

