%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1998-2011. All Rights Reserved.
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
%%%----------------------------------------------------------------------
%%% File    : mnesia_evil_backup.erl
%%% Author  : Dan Gudmundsson <dgud@legolas>
%%% Purpose : Evil backup tests
%%% Created : 3 Jun 1998 by Dan Gudmundsson <dgud@erix.ericsson.se>
%%%----------------------------------------------------------------------

-module(mnesia2_evil_backup).
-author('dgud@erix.ericsson.se').
-compile(export_all).
-include("mnesia2_test_lib.hrl").

%%-export([Function/Arity, ...]).

init_per_testcase(Func, Conf) ->
    mnesia2_test_lib:init_per_testcase(Func, Conf).

end_per_testcase(Func, Conf) ->
    mnesia2_test_lib:end_per_testcase(Func, Conf).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

all() -> 
    [backup, bad_backup, global_backup_checkpoint,
     {group, restore_tables}, traverse_backup,
     selective_backup_checkpoint,
     incremental_backup_checkpoint, install_fallback,
     uninstall_fallback, local_fallback,
     sops_with_checkpoint].

groups() -> 
    [{restore_tables, [],
      [restore_errors, restore_clear, restore_keep,
       restore_recreate, restore_clear_ram]}].

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, Config) ->
    Config.


backup(doc) -> ["Checking the interface to the function backup",
                "We don't check that the backups can be used here",
                "That is checked in install_fallback and in restore"];
backup(suite) -> [];
backup(Config) when is_list(Config) -> 
    [Node1, Node2] = _Nodes = ?acquire_nodes(2, Config),
    Tab = backup_tab,
    Def = [{disc_copies, [Node1]}, {ram_copies, [Node2]}],
    ?match({atomic, ok}, mnesia2:create_table(Tab, Def)),  
    ?match(ok, mnesia2:dirty_write({Tab, 1, test_ok})),
    File = "backup_test.BUP",
    ?match(ok, mnesia2:backup(File)),

    File2 = "backup_test2.BUP",
    Tab2 = backup_tab2,
    Def2 = [{disc_only_copies, [Node2]}],
    ?match({atomic, ok}, mnesia2:create_table(Tab2, Def2)),  
    ?match(ok, mnesia2:backup(File2, mnesia2_backup)),

    File3 = "backup_test3.BUP",
    mnesia2_test_lib:kill_mnesia2([Node2]),
    ?match({error, _}, mnesia2:backup(File3, mnesia2_backup)),

    ?match(ok, file:delete(File)),
    ?match(ok, file:delete(File2)),
    ?match({error, _}, file:delete(File3)),
    ?verify_mnesia2([Node1], [Node2]).


bad_backup(suite) -> [];
bad_backup(Config) when is_list(Config) -> 
    [Node1] = ?acquire_nodes(1, Config),
    Tab = backup_tab,
    Def = [{disc_copies, [Node1]}],
    ?match({atomic, ok}, mnesia2:create_table(Tab, Def)),
    ?match(ok, mnesia2:dirty_write({Tab, 1, test_ok})),
    File = "backup_test.BUP",
    ?match(ok, mnesia2:backup(File)),
    file:write_file(File, "trash", [append]),
    ?match(ok, mnesia2:dirty_write({Tab, 1, test_bad})),
    ?match({atomic,[Tab]}, mnesia2:restore(File, [{clear_tables, [Tab]}])),
    ?match([{Tab,1,test_ok}], mnesia2:dirty_read(Tab, 1)),
    
    ?match(ok, file:delete(File)),
    ?verify_mnesia2([Node1], []).



global_backup_checkpoint(doc) -> 
    ["Checking the interface to the function backup_checkpoint",
     "We don't check that the backups can be used here",
     "That is checked in install_fallback and in restore"];
global_backup_checkpoint(suite) -> [];
global_backup_checkpoint(Config) when is_list(Config) ->
    [Node1, Node2] = Nodes = ?acquire_nodes(2, Config),    
    Tab = backup_cp,
    Def = [{disc_copies, [Node1]}, {ram_copies, [Node2]}],
    File = "backup_checkpoint.BUP",
    File2 = "backup_checkpoint2.BUP",
    ?match({atomic, ok}, mnesia2:create_table(Tab, Def)),    
    ?match(ok, mnesia2:dirty_write({Tab, 1, test_ok})),
    ?match({error, _}, mnesia2:backup_checkpoint(cp_name, File)),
    Spec = [{name, cp_name}, {max,  mnesia2:system_info(tables)}],
    ?match({ok, _Name, _Ns}, mnesia2:activate_checkpoint(Spec)),
    ?match(ok, mnesia2:backup_checkpoint(cp_name, File)),
    ?match({error, _}, mnesia2:backup_checkpoint(cp_name_nonexist, File)),
    ?match(ok, mnesia2:backup_checkpoint(cp_name, File2, mnesia2_backup)),
    ?match({error, _}, file:delete(File)),
    ?match(ok, file:delete(File2)),
    ?verify_mnesia2(Nodes, []).


restore_errors(suite) -> [];
restore_errors(Config) when is_list(Config) ->
    [_Node] = ?acquire_nodes(1, Config),
    ?match({aborted, enoent}, mnesia2:restore(notAfile, [])),
    ?match({aborted, {badarg, _}}, mnesia2:restore(notAfile, not_a_list)),
    ?match({aborted, {badarg, _}}, mnesia2:restore(notAfile, [test_badarg])),
    ?match({aborted, {badarg, _}}, mnesia2:restore(notAfile, [{test_badarg, xxx}])),
    ?match({aborted, {badarg, _}}, mnesia2:restore(notAfile, [{skip_tables, xxx}])),
    ?match({aborted, {badarg, _}}, mnesia2:restore(notAfile, [{recreate_tables, [schema]}])),
    ?match({aborted, {badarg, _}}, mnesia2:restore(notAfile, [{default_op, asdklasd}])),
    Mnesia2Dir = mnesia2_lib:dir(),
    ?match({aborted, {not_a_log_file, _}}, mnesia2:restore(filename:join(Mnesia2Dir, "schema.DAT"), [])),
    ?match({aborted, _}, mnesia2:restore(filename:join(Mnesia2Dir, "LATEST.LOG"), [])),
    ok.

restore_clear(suite) -> [];
restore_clear(Config) when is_list(Config) ->
    restore(Config, clear_tables).

restore_keep(suite) -> [];
restore_keep(Config) when is_list(Config) ->
    restore(Config, keep_tables).

restore_recreate(suite) -> [];
restore_recreate(Config) when is_list(Config) ->
    restore(Config, recreate_tables).

check_tab(Records, Line) ->
    Verify = fun({Table, Key, Val}) -> 
		     case catch mnesia2:dirty_read({Table, Key}) of
			 [{Table, Key, Val}] -> ok;
			 Else ->
			     mnesia2_test_lib:error("Not matching on Node ~p ~n"
						   " Expected ~p~n Actual  ~p~n", 
						   [node(), {Table, Key, Val}, Else],
						   ?MODULE, Line),
			     exit(error)     
		     end;
		(Recs) ->
		     [{Tab, Key, _}, _] = Recs,
		     SRecs = lists:sort(Recs),
		     R_Recs = lists:sort(catch mnesia2:dirty_read({Tab, Key})),
		     case R_Recs of
			 SRecs -> ok;
			 Else ->
			     mnesia2_test_lib:error("Not matching on Node ~p ~n"
						   " Expected ~p~n Actual  ~p~n", 
						   [node(), SRecs, Else],
						   ?MODULE, Line),
			     exit(error)
		     end
	     end,
    lists:foreach(Verify, Records).

restore(Config, Op)  ->
    [Node1, Node2, _Node3] = Nodes = ?acquire_nodes(3, Config),
    
    Tab1 = ram_snmp,
    Def1 = [{snmp, [{key, integer}]}, {ram_copies, [Node1]}],
    Tab2 = disc_index,
    Def2 = [{index, [val]}, {disc_copies, [Node1, Node2]}],
    Tab3 = dionly_bag,
    Def3 = [{type, bag}, {disc_only_copies, Nodes}],
    ?match({atomic, ok}, mnesia2:create_table(Tab1, Def1)),
    ?match({atomic, ok}, mnesia2:create_table(Tab2, Def2)),
    ?match({atomic, ok}, mnesia2:create_table(Tab3, Def3)),

    File1 = "restore1.BUP",
    File2 = "restore2.BUP",    

    Restore = fun(O, A) ->
		      case mnesia2:restore(O, A) of
			  {atomic, Tabs} when is_list(Tabs) -> {atomic, lists:sort(Tabs)};
			  Other -> Other
		      end
	      end,
    Tabs = lists:sort([Tab1, Tab2, Tab3]),
    
    [mnesia2:dirty_write({Tab1, N, N+42}) || N <- lists:seq(1, 10)],
    [mnesia2:dirty_write({Tab2, N, N+43}) || N <- lists:seq(1, 10)],
    [mnesia2:dirty_write({Tab3, N, N+44}) || N <- lists:seq(1, 10)],
    
    Res1 = [{Tab1, N, N+42} || N <- lists:seq(1, 10)],
    Res2 = [{Tab2, N, N+43} || N <- lists:seq(1, 10)],
    Res3 = [{Tab3, N, N+44} || N <- lists:seq(1, 10)],
    
    {ok, Name, _} = mnesia2:activate_checkpoint([{min, Tabs}, {ram_overrides_dump, true}]),
    file:delete(File1),
    
    %% Test standard Restore on one table on one node
    ?match(ok, mnesia2:backup_checkpoint(Name, File1)),
    ?match(ok, mnesia2:deactivate_checkpoint(Name)),
    ?match(ok, mnesia2:backup(File2)),
    [mnesia2:dirty_write({Tab1, N, N+1}) || N <- lists:seq(1, 11)],
    [mnesia2:dirty_write({Tab2, N, N+1}) || N <- lists:seq(1, 11)],
    [mnesia2:dirty_write({Tab3, N, N+1}) || N <- lists:seq(1, 11)],

    Res21 = [{Tab2, N, N+1} || N <- lists:seq(1, 11)],
    Res31 = [[{Tab3, N, N+1}, {Tab3, N, N+44}] || N <- lists:seq(1, 10)],
    Check = fun() ->
		    [disk_log:pid2name(X) ||
			X <- processes(), Data <- [process_info(X, [current_function])],
			Data =/= undefined,
			element(1, element(2, lists:keyfind(current_function, 1, Data)))=:= disk_log]
	    end,
    Before = Check(),
    ?match({atomic, [Tab1]}, Restore(File1, [{Op, [Tab1]},
					     {skip_tables, Tabs -- [Tab1]}])),    
    case Op of 
	keep_tables -> 
	    ?match([{Tab1, 11, 12}], mnesia2:dirty_read({Tab1, 11}));
	clear_tables ->
	    ?match([], mnesia2:dirty_read({Tab1, 11}));
	recreate_tables ->
	    ?match([], mnesia2:dirty_read({Tab1, 11}))
    end,
    [rpc:call(Node, ?MODULE, check_tab, [Res1, ?LINE]) || Node <- Nodes],
    [rpc:call(Node, ?MODULE, check_tab, [Res21, ?LINE]) || Node <- Nodes],
    [rpc:call(Node, ?MODULE, check_tab, [Res31, ?LINE]) || Node <- Nodes],

    %% Restore all tables on it's nodes
    mnesia2:clear_table(Tab1),
    mnesia2:clear_table(Tab2),
    mnesia2:clear_table(Tab3),
    [mnesia2:dirty_write({Tab1, N, N+1}) || N <- lists:seq(1, 11)],
    [mnesia2:dirty_write({Tab2, N, N+1}) || N <- lists:seq(1, 11)],
    [mnesia2:dirty_write({Tab3, N, N+1}) || N <- lists:seq(1, 11)],

    ?match({atomic, ok}, mnesia2:del_table_copy(Tab2, Node1)),

    ?match({ok, Node1}, mnesia2:subscribe({table, Tab1})),
    
    ?match({atomic, Tabs}, Restore(File1, [{default_op, Op},
					   {module, mnesia2_backup}])),
    case Op of 
	clear_tables ->
	    ?match_receive({mnesia2_table_event, {delete, {schema, Tab1}, _}}),
	    ?match_receive({mnesia2_table_event, {write, {schema, Tab1, _}, _}}),
	    check_subscr(Tab1),
	    [rpc:call(Node, ?MODULE, check_tab, [Res1, ?LINE]) || Node <- Nodes],
	    [rpc:call(Node, ?MODULE, check_tab, [Res2, ?LINE]) || Node <- Nodes],
	    [rpc:call(Node, ?MODULE, check_tab, [Res3, ?LINE]) || Node <- Nodes],	  
	    ?match([], mnesia2:dirty_read({Tab1, 11})),
	    ?match([], mnesia2:dirty_read({Tab2, 11})),
	    ?match([], mnesia2:dirty_read({Tab3, 11})),
	    %% Check Index
	    ?match([{Tab2, 10, 53}], mnesia2:dirty_index_read(Tab2, 53, val)),
	    ?match([], mnesia2:dirty_index_read(Tab2, 11, val)),
	    %% Check Snmp
	    ?match({ok, [1]}, mnesia2:snmp_get_next_index(Tab1,[])),
	    ?match({ok, {Tab1, 1, 43}}, mnesia2:snmp_get_row(Tab1, [1])),
	    ?match(undefined, mnesia2:snmp_get_row(Tab1, [11])),
	    %% Check schema info
	    ?match([Node2], mnesia2:table_info(Tab2, where_to_write));
	keep_tables -> 
	    check_subscr(Tab1),
	    [rpc:call(Node, ?MODULE, check_tab, [Res1, ?LINE]) || Node <- Nodes],
	    [rpc:call(Node, ?MODULE, check_tab, [Res2, ?LINE]) || Node <- Nodes],
	    [rpc:call(Node, ?MODULE, check_tab, [Res31, ?LINE]) || Node <- Nodes],	    
	    ?match([{Tab1, 11, 12}], mnesia2:dirty_read({Tab1, 11})),
	    ?match([{Tab2, 11, 12}], mnesia2:dirty_read({Tab2, 11})),
	    ?match([{Tab3, 11, 12}], mnesia2:dirty_read({Tab3, 11})),
	    ?match([{Tab2, 10, 53}], mnesia2:dirty_index_read(Tab2, 53, val)),
	    %% Check Index
	    ?match([], mnesia2:dirty_index_read(Tab2, 11, val)),
	    ?match({ok, [1]}, mnesia2:snmp_get_next_index(Tab1,[])),
	    %% Check Snmp
	    ?match({ok, {Tab1, 1, 43}}, mnesia2:snmp_get_row(Tab1, [1])),
	    ?match({ok, {Tab1, 11, 12}}, mnesia2:snmp_get_row(Tab1, [11])),
	    %% Check schema info
	    ?match([Node2], mnesia2:table_info(Tab2, where_to_write));
	recreate_tables ->
	    check_subscr(Tab1, 0),
	    [rpc:call(Node, ?MODULE, check_tab, [Res1, ?LINE]) || Node <- Nodes],
	    [rpc:call(Node, ?MODULE, check_tab, [Res2, ?LINE]) || Node <- Nodes],
	    [rpc:call(Node, ?MODULE, check_tab, [Res3, ?LINE]) || Node <- Nodes],	
	    ?match([], mnesia2:dirty_read({Tab1, 11})),
	    ?match([], mnesia2:dirty_read({Tab2, 11})),
	    ?match([], mnesia2:dirty_read({Tab3, 11})),
	    %% Check Index
	    ?match([{Tab2, 10, 53}], mnesia2:dirty_index_read(Tab2, 53, val)),  	   	    
	    ?match([], mnesia2:dirty_index_read(Tab2, 11, val)),
	    %% Check Snmp
	    ?match({ok, [1]}, mnesia2:snmp_get_next_index(Tab1,[])),
	    ?match({ok, {Tab1, 1, 43}}, mnesia2:snmp_get_row(Tab1, [1])),
	    ?match(undefined, mnesia2:snmp_get_row(Tab1, [11])),
	    %% Check schema info
	    Ns = lists:sort([Node1, Node2]),
	    ?match(Ns, lists:sort(mnesia2:table_info(Tab2, where_to_write)))	    
    end,
    ?match(ok, file:delete(File1)),
    ?match(ok, file:delete(File2)),
    ?match([], Check() -- Before),

    ?verify_mnesia2(Nodes, []).


check_subscr(Tab) ->    
    check_subscr(Tab, 10).

check_subscr(_Tab, 0) ->    
    receive 
	Msg ->
	    ?error("Too many msgs ~p~n", [Msg])
    after 500 ->
	    ok
    end;
check_subscr(Tab, N) ->
    V = N +42,
    receive
	{mnesia2_table_event, {write, {Tab, N, V}, _}} ->
	    check_subscr(Tab, N-1)
    after 500 ->
	    ?error("Missing ~p~n", [{Tab, N, V}])
    end.

restore_clear_ram(suite) -> [];
restore_clear_ram(Config) when is_list(Config) ->
    Nodes = ?acquire_nodes(3, [{diskless, true}|Config]),
    
    ?match({atomic, ok}, mnesia2:create_table(a, [{ram_copies, Nodes}])),
    
    Write = fun(What) ->
		    mnesia2:write({a,1,What}),
		    mnesia2:write({a,2,What}),
		    mnesia2:write({a,3,What})
	    end,
    Bup = "restore_clear_ram.BUP",

    ?match({atomic, ok}, mnesia2:transaction(Write, [initial])),
    ?match({ok, _, _}, mnesia2:activate_checkpoint([{name,test}, 
						   {min, [schema, a]},
						   {ram_overrides_dump, true}])),
    ?match(ok, mnesia2:backup_checkpoint(test, Bup)),
    
    ?match({atomic, ok}, mnesia2:transaction(Write, [data])),
    ?match({atomic, [a]}, mnesia2:restore(Bup, [{clear_tables,[a]},{default_op,skip_tables}])),

    restore_clear_ram_loop(100, Nodes, Bup),
    
    ok.
    
restore_clear_ram_loop(N, Nodes = [N1,N2,N3], Bup) when N > 0 ->
    ?match([], mnesia2_test_lib:stop_mnesia2(Nodes)),
    ?match({_, []}, rpc:multicall([N1,N2], mnesia2, start, [[{extra_db_nodes, Nodes}]])),
    Key = rpc:async_call(N3, mnesia2, start, [[{extra_db_nodes, Nodes}]]),
    ?match({atomic, ok}, mnesia2:create_table(a, [{ram_copies, Nodes}])),
    ?match({atomic, [a]}, mnesia2:restore(Bup, [{clear_tables,[a]},{default_op,skip_tables}])),
    ?match(ok, rpc:yield(Key)),
    ?match(ok, rpc:call(N3, mnesia2, wait_for_tables, [[a], 3000])),
    case rpc:multicall(Nodes, mnesia2, table_info, [a,size]) of
	{[3,3,3], []} ->
	    restore_clear_ram_loop(N-1, Nodes, Bup);
	Error ->
	    ?match(3, Error)
    end;
restore_clear_ram_loop(_,_,_) ->
    ok.

traverse_backup(doc) -> 
    ["Testing the traverse_backup interface, the resulting file is not tested though",
     "See install_fallback for result using the output file from traverse_backup",
     "A side effect is that the backup file contents are tested"];
traverse_backup(suite) -> [];
traverse_backup(Config) when is_list(Config) -> 
    [Node1, Node2] = Nodes = ?acquire_nodes(2, Config),
    Tab = backup_tab,
    Def = [{disc_copies, [Node1]}, {ram_copies, [Node2]}],
    ?match({atomic, ok}, mnesia2:create_table(Tab, Def)),  
    ?match(ok, mnesia2:dirty_write({Tab, 1, test_nok})),
    ?match(ok, mnesia2:dirty_write({Tab, 2, test_nok})),
    ?match(ok, mnesia2:dirty_write({Tab, 3, test_nok})),
    ?match(ok, mnesia2:dirty_write({Tab, 4, test_nok})),
    ?match(ok, mnesia2:dirty_write({Tab, 5, test_nok})),
    File = "_treverse_backup.BUP",
    File2 = "traverse_backup2.BUP",
    File3 = "traverse_backup3.BUP",
    ?match(ok, mnesia2:backup(File)),
            
    Fun = fun({backup_tab, N, _}, Acc) -> {[{backup_tab, N, test_ok}], Acc+1};
             (Other, Acc) -> {[Other], Acc}
          end,

    ?match({ok, 5}, mnesia2:traverse_backup(File, read_only, Fun, 0)),
    ?match(ok, file:delete(read_only)),
    
    ?match({ok, 5}, mnesia2:traverse_backup(File, mnesia2_backup, 
                                           dummy, read_only, Fun, 0)),

    ?match({ok, 5}, mnesia2:traverse_backup(File, File2, Fun, 0)),    
    ?match({ok, 5}, mnesia2:traverse_backup(File2, mnesia2_backup, 
                                           File3, mnesia2_backup, Fun, 0)),
    
    BadFun = fun({bad_tab, _N, asd}, Acc) -> {{error, error}, Acc} end,    
    ?match({error, _}, mnesia2:traverse_backup(File, read_only, BadFun, 0)),    
    ?match({error, _}, file:delete(read_only)),
    ?match(ok, file:delete(File)),
    ?match(ok, file:delete(File2)),
    ?match(ok, file:delete(File3)),
    ?verify_mnesia2(Nodes, []).


install_fallback(doc) -> 
    ["This tests the install_fallback intf.",
     "It also verifies that the output from backup_checkpoint and traverse_backup",
     "is valid"];
install_fallback(suite) -> [];
install_fallback(Config) when is_list(Config) ->
    [Node1, Node2] = Nodes = ?acquire_nodes(2, Config),
    Tab = fallbacks_test,
    Def = [{disc_copies, [Node1]}, {ram_copies, [Node2]}],
    ?match({atomic, ok}, mnesia2:create_table(Tab, Def)),  
    ?match(ok, mnesia2:dirty_write({Tab, 1, test_nok})),
    ?match(ok, mnesia2:dirty_write({Tab, 2, test_nok})),
    ?match(ok, mnesia2:dirty_write({Tab, 3, test_nok})),
    ?match(ok, mnesia2:dirty_write({Tab, 4, test_nok})),
    ?match(ok, mnesia2:dirty_write({Tab, 5, test_nok})),

    Tab2 = fallbacks_test2,
    Def2 = [{disc_copies, [node()]}],
    ?match({atomic, ok}, mnesia2:create_table(Tab2, Def2)),  
    Tab3 = fallbacks_test3,
    ?match({atomic, ok}, mnesia2:create_table(Tab3, Def2)),  
    Fun2 = fun(Key) ->
		  Rec = {Tab2, Key, test_ok},
		  mnesia2:dirty_write(Rec),
		  [Rec]
	  end,
    TabSize3 = 1000,
    OldRecs2 = [Fun2(K) || K <- lists:seq(1, TabSize3)],
    
    Spec =[{name, cp_name}, {max,  mnesia2:system_info(tables)}],
    ?match({ok, _Name, Nodes}, mnesia2:activate_checkpoint(Spec)),
    ?match(ok, mnesia2:dirty_write({Tab, 6, test_nok})),
    [mnesia2:dirty_write({Tab2, K, test_nok}) || K <- lists:seq(1, TabSize3 + 10)],
    File = "install_fallback.BUP",
    File2 = "install_fallback2.BUP",
    File3 = "install_fallback3.BUP",
    ?match(ok, mnesia2:backup_checkpoint(cp_name, File)),
        
    Fun = fun({T, N, _}, Acc) when T == Tab ->
		  case N rem 2 of 
		      0 -> 
			  io:format("write ~p -> ~p~n", [N, T]),
			  {[{T, N, test_ok}], Acc + 1};
		      1 ->
			  io:format("write ~p -> ~p~n", [N, Tab3]),
			  {[{Tab3, N, test_ok}], Acc + 1}
		  end;
             ({T, N}, Acc) when T == Tab ->
		  case N rem 2 of 
		      0 -> 
			  io:format("delete ~p -> ~p~n", [N, T]),
			  {[{T, N}], Acc + 1};
		      1 ->
			  io:format("delete ~p -> ~p~n", [N, Tab3]),
			  {[{Tab3, N}], Acc + 1}
		  end;
             (Other, Acc) ->
		  {[Other], Acc}
          end,
    ?match({ok, _}, mnesia2:traverse_backup(File, File2, Fun, 0)),
    ?match(ok, mnesia2:install_fallback(File2)),
    
    mnesia2_test_lib:kill_mnesia2([Node1, Node2]),
    timer:sleep(timer:seconds(1)), % Let it die!

    ok = mnesia2:start([{ignore_fallback_at_startup, true}]),
    ok = mnesia2:wait_for_tables([Tab, Tab2, Tab3], 10000),
    ?match([{Tab, 6, test_nok}], mnesia2:dirty_read({Tab, 6})),
    mnesia2_test_lib:kill_mnesia2([Node1]),
    application:set_env(mnesia2, ignore_fallback_at_startup, false),

    timer:sleep(timer:seconds(1)), % Let it die!

    ?match([], mnesia2_test_lib:start_mnesia2([Node1, Node2], [Tab, Tab2, Tab3])),

    % Verify 
    ?match([], mnesia2:dirty_read({Tab, 1})),
    ?match([{Tab3, 1, test_ok}], mnesia2:dirty_read({Tab3, 1})),
    ?match([{Tab, 2, test_ok}], mnesia2:dirty_read({Tab, 2})),
    ?match([], mnesia2:dirty_read({Tab3, 2})),
    ?match([], mnesia2:dirty_read({Tab, 3})),
    ?match([{Tab3, 3, test_ok}], mnesia2:dirty_read({Tab3, 3})),
    ?match([{Tab, 4, test_ok}], mnesia2:dirty_read({Tab, 4})),
    ?match([], mnesia2:dirty_read({Tab3, 4})),
    ?match([], mnesia2:dirty_read({Tab, 5})),   
    ?match([{Tab3, 5, test_ok}], mnesia2:dirty_read({Tab3, 5})),   
    ?match([], mnesia2:dirty_read({Tab, 6})),   
    ?match([], mnesia2:dirty_read({Tab3, 6})),   
    ?match([], [mnesia2:dirty_read({Tab2, K}) || K <- lists:seq(1, TabSize3)] -- OldRecs2),
    ?match(TabSize3, mnesia2:table_info(Tab2, size)),

    % Check the interface
    file:delete(File3),
    ?match({error, _}, mnesia2:install_fallback(File3)),
    ?match({error, _}, mnesia2:install_fallback(File2, mnesia2_badmod)),
    ?match({error, _}, mnesia2:install_fallback(File2, {foo, foo})),
    ?match({error, _}, mnesia2:install_fallback(File2, [{foo, foo}])),
    ?match({error, {badarg, {skip_tables, _}}},
	   mnesia2:install_fallback(File2, [{default_op, skip_tables},
					   {default_op, keep_tables},
					   {keep_tables, [Tab, Tab2, Tab3]},
					   {skip_tables, [foo,{asd}]}])),
    ?match(ok, mnesia2:install_fallback(File2, mnesia2_backup)),
    ?match(ok, file:delete(File)),
    ?match(ok, file:delete(File2)),
    ?match({error, _}, file:delete(File3)),
    ?verify_mnesia2(Nodes, []).

uninstall_fallback(suite) -> [];
uninstall_fallback(Config) when is_list(Config) ->
    [Node1, Node2] = Nodes = ?acquire_nodes(2, Config),
    Tab = uinst_fallbacks_test,
    File = "uinst_fallback.BUP",
    File2 = "uinst_fallback2.BUP",
    Def = [{disc_copies, [Node1]}, {ram_copies, [Node2]}],
    ?match({atomic, ok}, mnesia2:create_table(Tab, Def)),  
    ?match(ok, mnesia2:dirty_write({Tab, 1, test_ok})),
    ?match(ok, mnesia2:backup(File)),
    Fun = fun({T, N, _}, Acc) when T == Tab -> 
                  {[{T, N, test_nok}], Acc+1};
             (Other, Acc) -> {[Other], Acc}
          end,
    ?match({ok, _}, mnesia2:traverse_backup(File, File2, Fun, 0)),
    ?match({error, enoent}, mnesia2:uninstall_fallback()),
    ?match(ok, mnesia2:install_fallback(File2)),
    ?match(ok, file:delete(File)),
    ?match(ok, file:delete(File2)),
    ?match({error, _}, mnesia2:uninstall_fallback([foobar])),
    ?match(ok, mnesia2:uninstall_fallback()),
    
    mnesia2_test_lib:kill_mnesia2([Node1, Node2]),
    timer:sleep(timer:seconds(1)), % Let it die!
    ?match([], mnesia2_test_lib:start_mnesia2([Node1, Node2], [Tab])),
    ?match([{Tab, 1, test_ok}], mnesia2:dirty_read({Tab, 1})),
    ?verify_mnesia2(Nodes, []).

local_fallback(suite) -> [];
local_fallback(Config) when is_list(Config) ->
    [Node1, Node2] = Nodes = ?acquire_nodes(2, Config),
    Tab = local_fallback,
    File = "local_fallback.BUP",
    Def = [{disc_copies, Nodes}],
    Key = foo,
    Pre =  {Tab, Key, pre},
    Post =  {Tab, Key, post},
    ?match({atomic, ok}, mnesia2:create_table(Tab, Def)),  
    ?match(ok, mnesia2:dirty_write(Pre)),
    ?match(ok, mnesia2:backup(File)),
    ?match(ok, mnesia2:dirty_write(Post)),
    Local = [{scope, local}],
    ?match({error, enoent}, mnesia2:uninstall_fallback(Local)),
    ?match(ok, mnesia2:install_fallback(File, Local)),
    ?match(true, mnesia2:system_info(fallback_activated)),
    ?match(ok, mnesia2:uninstall_fallback(Local)),
    ?match(false, mnesia2:system_info(fallback_activated)),
    ?match(ok, mnesia2:install_fallback(File, Local)),
    ?match(true, mnesia2:system_info(fallback_activated)),
    
    ?match(false, rpc:call(Node2, mnesia2, system_info , [fallback_activated])),
    ?match(ok, rpc:call(Node2, mnesia2, install_fallback , [File, Local])),
    ?match([Post], mnesia2:dirty_read({Tab, Key})),
    ?match([Post], rpc:call(Node2, mnesia2, dirty_read, [{Tab, Key}])),

    ?match([], mnesia2_test_lib:kill_mnesia2(Nodes)),
    ?match([], mnesia2_test_lib:start_mnesia2(Nodes, [Tab])),
    ?match([Pre], mnesia2:dirty_read({Tab, Key})),
    ?match([Pre], rpc:call(Node2, mnesia2, dirty_read, [{Tab, Key}])),
    Dir = rpc:call(Node2, mnesia2, system_info , [directory]),

    ?match(ok, mnesia2:dirty_write(Post)),
    ?match([Post], mnesia2:dirty_read({Tab, Key})),
    ?match([], mnesia2_test_lib:kill_mnesia2([Node2])),
    ?match(ok, mnesia2:install_fallback(File, Local ++ [{mnesia2_dir, Dir}])),
    ?match([], mnesia2_test_lib:kill_mnesia2([Node1])),
    
    ?match([], mnesia2_test_lib:start_mnesia2([Node2], [])),
    ?match(yes, rpc:call(Node2, mnesia2, force_load_table, [Tab])),
    ?match([], mnesia2_test_lib:start_mnesia2(Nodes, [Tab])), 
    ?match([Pre], mnesia2:dirty_read({Tab, Key})),
   
    ?match(ok, file:delete(File)),  
    ?verify_mnesia2(Nodes, []).
    
selective_backup_checkpoint(doc) -> 
    ["Perform a selective backup of a checkpoint"];
selective_backup_checkpoint(suite) -> [];
selective_backup_checkpoint(Config) when is_list(Config) ->
    [Node1, Node2] = Nodes = ?acquire_nodes(2, Config),    
    Tab = sel_backup,
    OmitTab = sel_backup_omit,
    CpName = sel_cp,
    Def = [{disc_copies, [Node1, Node2]}],
    File = "selective_backup_checkpoint.BUP",
    ?match({atomic, ok}, mnesia2:create_table(Tab, Def)),    
    ?match({atomic, ok}, mnesia2:create_table(OmitTab, Def)),    
    ?match(ok, mnesia2:dirty_write({Tab, 1, test_ok})),
    ?match(ok, mnesia2:dirty_write({OmitTab, 1, test_ok})),
    CpSpec = [{name, CpName}, {max,  mnesia2:system_info(tables)}],
    ?match({ok, CpName, _Ns}, mnesia2:activate_checkpoint(CpSpec)),

    BupSpec = [{tables, [Tab]}],
    ?match(ok, mnesia2:backup_checkpoint(CpName, File, BupSpec)),

    ?match([schema, sel_backup], bup_tables(File, mnesia2_backup)),
    ?match(ok, file:delete(File)),

    BupSpec2 = [{tables, [Tab, OmitTab]}],
    ?match(ok, mnesia2:backup_checkpoint(CpName, File, BupSpec2)),

    ?match([schema, sel_backup, sel_backup_omit],
	   bup_tables(File, mnesia2_backup)),
    ?match(ok, file:delete(File)),
    ?verify_mnesia2(Nodes, []).

bup_tables(File, Mod) ->
    Fun = fun(Rec, Tabs) ->
		  Tab = element(1, Rec),
		  Tabs2 = [Tab | lists:delete(Tab, Tabs)],
		  {[Rec], Tabs2}
	  end,
    case mnesia2:traverse_backup(File, Mod, dummy, read_only, Fun, []) of
	{ok, Tabs} ->
	    lists:sort(Tabs);
	{error, Reason} ->
	    exit(Reason)
    end.

incremental_backup_checkpoint(doc) -> 
    ["Perform a incremental backup of a checkpoint"];
incremental_backup_checkpoint(suite) -> [];
incremental_backup_checkpoint(Config) when is_list(Config) ->
    [Node1] = Nodes = ?acquire_nodes(1, Config),    
    Tab = incr_backup,
    Def = [{disc_copies, [Node1]}],
    ?match({atomic, ok}, mnesia2:create_table(Tab, Def)),
    OldRecs = [{Tab, K, -K} || K <- lists:seq(1, 5)],
    ?match([ok|_], [mnesia2:dirty_write(R) || R <- OldRecs]),
    OldCpName = old_cp,
    OldCpSpec = [{name, OldCpName}, {min,  [Tab]}],
    ?match({ok, OldCpName, _Ns}, mnesia2:activate_checkpoint(OldCpSpec)),

    BupSpec = [{tables, [Tab]}],
    OldFile = "old_full_backup.BUP",
    ?match(ok, mnesia2:backup_checkpoint(OldCpName, OldFile, BupSpec)),
    ?match(OldRecs, bup_records(OldFile, mnesia2_backup)),
    ?match(ok, mnesia2:dirty_delete({Tab, 1})),
    ?match(ok, mnesia2:dirty_write({Tab, 2, 2})),
    ?match(ok, mnesia2:dirty_write({Tab, 3, -3})),

    NewCpName = new_cp,
    NewCpSpec = [{name, NewCpName}, {min,  [Tab]}],
    ?match({ok, NewCpName, _Ns}, mnesia2:activate_checkpoint(NewCpSpec)),
    ?match(ok, mnesia2:dirty_write({Tab, 4, 4})),

    NewFile = "new_full_backup.BUP",
    ?match(ok, mnesia2:backup_checkpoint(NewCpName, NewFile, BupSpec)),
    NewRecs = [{Tab, 2, 2}, {Tab, 3, -3},
	       {Tab, 4, 4}, {Tab, 4}, {Tab, 4, -4}, {Tab, 5, -5}],
    ?match(NewRecs, bup_records(NewFile, mnesia2_backup)),

    DiffFile = "diff_backup.BUP",
    DiffBupSpec = [{tables, [Tab]}, {incremental, OldCpName}],
    ?match(ok, mnesia2:backup_checkpoint(NewCpName, DiffFile, DiffBupSpec)),
    DiffRecs = [{Tab, 1}, {Tab, 2}, {Tab, 2, 2}, {Tab, 3}, {Tab, 3, -3},
		{Tab, 4}, {Tab, 4, 4}, {Tab, 4}, {Tab, 4, -4}],
    ?match(DiffRecs, bup_records(DiffFile, mnesia2_backup)),

    ?match(ok, mnesia2:deactivate_checkpoint(OldCpName)),
    ?match(ok, mnesia2:deactivate_checkpoint(NewCpName)),
    ?match(ok, file:delete(OldFile)),
    ?match(ok, file:delete(NewFile)),
    ?match(ok, file:delete(DiffFile)),

    ?verify_mnesia2(Nodes, []).

bup_records(File, Mod) ->
    Fun = fun(Rec, Recs) when element(1, Rec) == schema ->
		  {[Rec], Recs};
	     (Rec, Recs) ->
		  {[Rec], [Rec | Recs]}
	  end,
    case mnesia2:traverse_backup(File, Mod, dummy, read_only, Fun, []) of
	{ok, Recs} ->
	    lists:keysort(1, lists:keysort(2, lists:reverse(Recs)));
	{error, Reason} ->
	    exit(Reason)
    end.

sops_with_checkpoint(doc) -> 
    ["Test schema operations during a checkpoint"];
sops_with_checkpoint(suite) -> [];
sops_with_checkpoint(Config) when is_list(Config) ->
    Ns = ?acquire_nodes(2, Config),
    
    ?match({ok, cp1, Ns}, mnesia2:activate_checkpoint([{name, cp1},{max,mnesia2:system_info(tables)}])),
    Tab = tab, 
    ?match({atomic, ok}, mnesia2:create_table(Tab, [{disc_copies,Ns}])),
    OldRecs = [{Tab, K, -K} || K <- lists:seq(1, 5)],
    [mnesia2:dirty_write(R) || R <- OldRecs],
    
    ?match({ok, cp2, Ns}, mnesia2:activate_checkpoint([{name, cp2},{max,mnesia2:system_info(tables)}])),
    File1 = "cp1_delete_me.BUP",
    ?match(ok, mnesia2:dirty_write({Tab,6,-6})),
    ?match(ok, mnesia2:backup_checkpoint(cp1, File1)),
    ?match(ok, mnesia2:dirty_write({Tab,7,-7})),
    File2 = "cp2_delete_me.BUP",
    ?match(ok, mnesia2:backup_checkpoint(cp2, File2)),
    
    ?match(ok, mnesia2:deactivate_checkpoint(cp1)),
    ?match(ok, mnesia2:backup_checkpoint(cp2, File1)),
    ?match(ok, mnesia2:dirty_write({Tab,8,-8})),
    
    ?match({atomic,ok}, mnesia2:delete_table(Tab)),
    ?match({error,_}, mnesia2:backup_checkpoint(cp2, File2)),
    ?match({'EXIT',_}, mnesia2:dirty_write({Tab,9,-9})),

    ?match({atomic,_}, mnesia2:restore(File1, [{default_op, recreate_tables}])), 
    Test = fun(N) when N > 5 -> ?error("To many records in backup ~p ~n", [N]);
	      (N) -> case mnesia2:dirty_read(Tab,N) of
			 [{Tab,N,B}] when -B =:= N -> ok;
			 Other -> ?error("Not matching ~p ~p~n", [N,Other])
		     end
	   end,
    [Test(N) || N <- mnesia2:dirty_all_keys(Tab)],
    ?match({aborted,enoent}, mnesia2:restore(File2, [{default_op, recreate_tables}])), 
    
    file:delete(File1), file:delete(File2),

    ?verify_mnesia2(Ns, []).
