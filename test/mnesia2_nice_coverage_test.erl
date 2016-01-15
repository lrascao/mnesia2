%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2014. All Rights Reserved.
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
-module(mnesia2_nice_coverage_test).
-author('hakan@erix.ericsson.se').
-compile([export_all]).
-include("mnesia2_test_lib.hrl").

-record(nice_tab, {key, val}).

init_per_testcase(Func, Conf) ->
    mnesia2_test_lib:init_per_testcase(Func, Conf).

end_per_testcase(Func, Conf) ->
    mnesia2_test_lib:end_per_testcase(Func, Conf).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
all() -> 
    [nice].

groups() -> 
    [].

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, Config) ->
    Config.


nice(doc) -> [""];
nice(suite) -> [];
nice(Config) when is_list(Config) ->
    %% The whole test suite is one huge test case for the time beeing
    
    [Node1, Node2] = Nodes = ?acquire_nodes(2, Config), 
    Attrs = record_info(fields, nice_tab), 
    
    initialize(Attrs, Node1), 
    dirty_access(Node1), 
    success_and_fail(), 
    index_mgt(), 
    
    adm(Attrs, Node1, Node2), 
    snmp(Node1, Node2), 
    backup(Node1), 
    ?verify_mnesia2(Nodes, []).

initialize(Attrs, Node1) ->
    ?match(Version when is_list(Version), mnesia2:system_info(version)), 

    Schema = [{name, nice_tab}, 
	      {attributes, Attrs}, {ram_copies, [Node1]}], 

    ?match({_, _}, mnesia2:system_info(schema_version)), 
    ?match({atomic, ok}, mnesia2:create_table(Schema)), 

    ?match(ok, mnesia2:info()), 
    ?match(set, mnesia2:table_info(nice_tab, type)), 
    ?match(ok, mnesia2:schema()), 
    ?match(ok, mnesia2:schema(nice_tab)),
    ok.

dirty_access(Node1) ->
    TwoThree = #nice_tab{key=23, val=23}, 
    TwoFive  = #nice_tab{key=25, val=25}, 
    ?match([], mnesia2:dirty_slot(nice_tab, 0)), 
    ?match(ok, mnesia2:dirty_write(TwoThree)), 
    ?match([TwoThree], mnesia2:dirty_read({nice_tab, 23})), 
    ?match(ok, mnesia2:dirty_write(TwoFive)), 
    ?match(ok, mnesia2:dirty_delete_object(TwoFive)), 

    ?match(23, mnesia2:dirty_first(nice_tab)), 
    ?match('$end_of_table', mnesia2:dirty_next(nice_tab, 23)), 
    ?match([TwoThree], mnesia2:dirty_match_object(TwoThree)), 
    ?match(ok, mnesia2:dirty_delete({nice_tab, 23})), 

    CounterSchema = [{ram_copies, [Node1]}], 
    ?match({atomic, ok}, mnesia2:create_table(nice_counter_tab, CounterSchema)), 
    TwoFour  = {nice_counter_tab, 24, 24}, 
    ?match(ok, mnesia2:dirty_write(TwoFour)), 
    ?match(34, mnesia2:dirty_update_counter({nice_counter_tab, 24}, 10)), 
    TF = {nice_counter_tab, 24, 34}, 
    ?match([TF], mnesia2:dirty_read({nice_counter_tab, 24})), 
    ?match(ok, mnesia2:dirty_delete({nice_counter_tab, 24})), 
    ?match(ok, mnesia2:dirty_delete_object(TF)),
    ok.

success_and_fail() ->
    ?match({atomic, a_good_trans}, mnesia2:transaction(fun() ->good_trans()end)), 

    BadFun =
	fun() ->
		Two = #nice_tab{key=2, val=12}, 
		?match([Two], mnesia2:match_object(#nice_tab{key='$1', val=12})), 
		?match([#nice_tab{key=3, val=13}], mnesia2:wread({nice_tab, 3})), 
		?match(ok, mnesia2:delete({nice_tab, 1})), 
		?match(ok, mnesia2:delete_object(Two)), 
		mnesia2:abort(bad_trans), 
		?match(bad, trans)
	end, 
    ?match({aborted, bad_trans}, mnesia2:transaction(BadFun)), 
    ?match(L when is_list(L), mnesia2:error_description(no_exists)), 
    ?match({atomic, ok}, mnesia2:transaction(fun(A) -> lock(), A end, [ok])), 
    ?match({atomic, ok}, mnesia2:transaction(fun(A) -> lock(), A end, [ok], 3)),
    ok.

good_trans() ->
    ?match([], mnesia2:read(nice_tab, 3)), 
    ?match([], mnesia2:read({nice_tab, 3})), 
    ?match(ok, mnesia2:write(#nice_tab{key=14, val=4})), 
    ?match([14], mnesia2:all_keys(nice_tab)), 

    Records = [ #nice_tab{key=K, val=K+10} || K <- lists:seq(1, 10) ], 
    Ok = [ ok || _ <- Records], 
    ?match(Ok, lists:map(fun(R) -> mnesia2:write(R) end, Records)), 
    a_good_trans.


lock() ->
    ?match(ok, mnesia2:s_write(#nice_tab{key=22, val=22})), 
    ?match(ok, mnesia2:read_lock_table(nice_tab)), 
    ?match(ok, mnesia2:write_lock_table(nice_tab)),
    ok.

index_mgt() ->
    UniversalRec = #nice_tab{key=4711, val=4711}, 
    ?match(ok, mnesia2:dirty_write(UniversalRec)), 
    ValPos = #nice_tab.val, 
    ?match({atomic, ok}, mnesia2:add_table_index(nice_tab, ValPos)), 

    IndexFun =
	fun() ->
		?match([UniversalRec], 
		       mnesia2:index_read(nice_tab, 4711, ValPos)), 
		Pat = #nice_tab{key='$1', val=4711}, 
		?match([UniversalRec], 
		       mnesia2:index_match_object(Pat, ValPos)), 
		index_trans
	end, 
    ?match({atomic, index_trans}, mnesia2:transaction(IndexFun, infinity)), 
    ?match([UniversalRec], 
	   mnesia2:dirty_index_read(nice_tab, 4711, ValPos)), 
    ?match([UniversalRec], 
	   mnesia2:dirty_index_match_object(#nice_tab{key='$1', val=4711}, ValPos)), 

    ?match({atomic, ok}, mnesia2:del_table_index(nice_tab, ValPos)),
    ok.

adm(Attrs, Node1, Node2) ->
    This = node(), 
    ?match({ok, This}, mnesia2:subscribe(system)), 
    ?match({atomic, ok}, 
	   mnesia2:add_table_copy(nice_tab, Node2, disc_only_copies)), 
    ?match({atomic, ok}, 
	   mnesia2:change_table_copy_type(nice_tab, Node2, ram_copies)), 
    ?match({atomic, ok}, mnesia2:del_table_copy(nice_tab, Node1)), 
    ?match(stopped, rpc:call(Node1, mnesia2, stop, [])), 
    ?match([], mnesia2_test_lib:start_mnesia2([Node1, Node2], [nice_tab])), 
    ?match(ok, mnesia2:wait_for_tables([schema], infinity)), 

    Transformer = fun(Rec) ->
			  list_to_tuple(tuple_to_list(Rec) ++ [initial_value])
		  end, 
    ?match({atomic, ok}, 
	   mnesia2:transform_table(nice_tab, Transformer, Attrs ++ [extra])), 

    ?match({atomic, ok}, mnesia2:delete_table(nice_tab)), 
    DumpSchema = [{name, nice_tab}, {attributes, Attrs}, {ram_copies, [Node2]}], 
    ?match({atomic, ok}, mnesia2:create_table(DumpSchema)), 
    ?match({atomic, ok}, mnesia2:dump_tables([nice_tab])), 
    ?match({atomic, ok}, mnesia2:move_table_copy(nice_tab, Node2, Node1)), 

    ?match(yes, mnesia2:force_load_table(nice_counter_tab)), 
    ?match(ok, mnesia2:sync_log()),
    ?match(dumped, mnesia2:dump_log()),
    ok.

backup(Node1) ->
    Tab = backup_nice,
    Def = [{disc_copies, [Node1]}], 
    ?match({atomic, ok}, mnesia2:create_table(Tab, Def)),    
    ?match({ok,_,_}, mnesia2:activate_checkpoint([{name, cp}, {max, [Tab]}])), 
    File = "nice_backup.BUP",
    File2 = "nice_backup2.BUP",
    File3 = "nice_backup3.BUP",
    ?match(ok, mnesia2:backup_checkpoint(cp, File)), 
    ?match(ok, mnesia2:backup_checkpoint(cp, File, mnesia2_backup)), 
    ?match(ok, mnesia2:deactivate_checkpoint(cp)), 
    ?match(ok, mnesia2:backup(File)), 
    ?match(ok, mnesia2:backup(File, mnesia2_backup)), 

    Fun = fun(X, Acc) -> {[X], Acc} end,
    ?match({ok, 0}, mnesia2:traverse_backup(File, File2, Fun, 0)), 
    ?match({ok, 0}, mnesia2:traverse_backup(File, mnesia2_backup, dummy, read_only, Fun, 0)), 
    ?match(ok, mnesia2:install_fallback(File)), 
    ?match(ok, mnesia2:uninstall_fallback()), 
    ?match(ok, mnesia2:install_fallback(File, mnesia2_backup)), 
    ?match(ok, mnesia2:dump_to_textfile(File3)), 
    ?match({atomic, ok}, mnesia2:load_textfile(File3)),
    ?match(ok, file:delete(File)),
    ?match(ok, file:delete(File2)),
    ?match(ok, file:delete(File3)),
    ok.

snmp(Node1, Node2) ->
    Tab = nice_snmp, 
    Def = [{disc_copies, [Node1]}, {ram_copies, [Node2]}], 
    ?match({atomic, ok}, mnesia2:create_table(Tab, Def)),
    ?match({aborted, {badarg, Tab, _}}, mnesia2:snmp_open_table(Tab, [])),
    ?match({atomic, ok}, mnesia2:snmp_open_table(Tab, [{key, integer}])),
    ?match(endOfTable, mnesia2:snmp_get_next_index(Tab, [0])), 
    ?match(undefined, mnesia2:snmp_get_row(Tab, [0])), 
    ?match(undefined, mnesia2:snmp_get_mnesia2_key(Tab, [0])), 
    ?match({atomic, ok}, mnesia2:snmp_close_table(Tab)),
    ok.

