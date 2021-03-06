%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1998-2014. All Rights Reserved.
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
-module(mnesia2_schema_recovery_test).
-author('hakan@erix.ericsson.se').
-compile([export_all]).
-include("mnesia2_test_lib.hrl").

init_per_testcase(Func, Conf) ->
    mnesia2_test_lib:init_per_testcase(Func, Conf).

end_per_testcase(Func, Conf) ->
    mnesia2_test_lib:end_per_testcase(Func, Conf).

-define(receive_messages(Msgs), receive_messages(Msgs, ?FILE, ?LINE)).

% First Some debug logging 
-define(dgb, true).
-ifdef(dgb).
-define(dl(X, Y), ?verbose("**TRACING: " ++ X ++ "**~n", Y)).
-else. 
-define(dl(X, Y), ok).
-endif.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

all() -> 
    [{group, interrupted_before_log_dump},
     {group, interrupted_after_log_dump}].

groups() -> 
    [{interrupted_before_log_dump, [],
      [interrupted_before_create_ram,
       interrupted_before_create_disc,
       interrupted_before_create_do,
       interrupted_before_create_nostore,
       interrupted_before_delete_ram,
       interrupted_before_delete_disc,
       interrupted_before_delete_do,
       interrupted_before_add_ram,
       interrupted_before_add_disc,
       interrupted_before_add_do,
       interrupted_before_add_kill_copier,
       interrupted_before_move_ram,
       interrupted_before_move_disc,
       interrupted_before_move_do,
       interrupted_before_move_kill_copier,
       interrupted_before_delcopy_ram,
       interrupted_before_delcopy_disc,
       interrupted_before_delcopy_do,
       interrupted_before_delcopy_kill_copier,
       interrupted_before_addindex_ram,
       interrupted_before_addindex_disc,
       interrupted_before_addindex_do,
       interrupted_before_delindex_ram,
       interrupted_before_delindex_disc,
       interrupted_before_delindex_do,
       interrupted_before_change_type_ram2disc,
       interrupted_before_change_type_ram2do,
       interrupted_before_change_type_disc2ram,
       interrupted_before_change_type_disc2do,
       interrupted_before_change_type_do2ram,
       interrupted_before_change_type_do2disc,
       interrupted_before_change_type_other_node,
       interrupted_before_change_schema_type]},
     {interrupted_after_log_dump, [],
      [interrupted_after_create_ram,
       interrupted_after_create_disc,
       interrupted_after_create_do,
       interrupted_after_create_nostore,
       interrupted_after_delete_ram,
       interrupted_after_delete_disc,
       interrupted_after_delete_do,
       interrupted_after_add_ram,
       interrupted_after_add_disc,
       interrupted_after_add_do,
       interrupted_after_add_kill_copier,
       interrupted_after_move_ram,
       interrupted_after_move_disc,
       interrupted_after_move_do,
       interrupted_after_move_kill_copier,
       interrupted_after_delcopy_ram,
       interrupted_after_delcopy_disc,
       interrupted_after_delcopy_do,
       interrupted_after_delcopy_kill_copier,
       interrupted_after_addindex_ram,
       interrupted_after_addindex_disc,
       interrupted_after_addindex_do,
       interrupted_after_delindex_ram,
       interrupted_after_delindex_disc,
       interrupted_after_delindex_do,
       interrupted_after_change_type_ram2disc,
       interrupted_after_change_type_ram2do,
       interrupted_after_change_type_disc2ram,
       interrupted_after_change_type_disc2do,
       interrupted_after_change_type_do2ram,
       interrupted_after_change_type_do2disc,
       interrupted_after_change_type_other_node,
       interrupted_after_change_schema_type]}].

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, Config) ->
    Config.

interrupted_before_create_ram(doc) -> [""];
interrupted_before_create_ram(suite) -> [];
interrupted_before_create_ram(Config) when is_list(Config) ->
    KillAt = {mnesia2_dumper, dump_schema_op},
    interrupted_create(Config, ram_copies, all, KillAt).

interrupted_before_create_disc(doc) -> [""];
interrupted_before_create_disc(suite) -> [];
interrupted_before_create_disc(Config) when is_list(Config) ->
    KillAt = {mnesia2_dumper, dump_schema_op},
    interrupted_create(Config, disc_copies, all, KillAt).

interrupted_before_create_do(doc) -> [""];
interrupted_before_create_do(suite) -> [];
interrupted_before_create_do(Config) when is_list(Config) ->
    KillAt = {mnesia2_dumper, dump_schema_op},
    interrupted_create(Config, disc_only_copies, all, KillAt).

interrupted_before_create_nostore(doc) -> [""];
interrupted_before_create_nostore(suite) -> [];
interrupted_before_create_nostore(Config) when is_list(Config) ->
    KillAt = {mnesia2_dumper, dump_schema_op},
    interrupted_create(Config, ram_copies, one, KillAt).

interrupted_after_create_ram(doc) -> [""];
interrupted_after_create_ram(suite) -> [];
interrupted_after_create_ram(Config) when is_list(Config) ->
    KillAt = {mnesia2_dumper, post_dump},
    interrupted_create(Config, ram_copies, all, KillAt).

interrupted_after_create_disc(doc) -> [""];
interrupted_after_create_disc(suite) -> [];
interrupted_after_create_disc(Config) when is_list(Config) ->
    KillAt = {mnesia2_dumper, post_dump},
    interrupted_create(Config, disc_copies, all, KillAt).

interrupted_after_create_do(doc) -> [""];
interrupted_after_create_do(suite) -> [];
interrupted_after_create_do(Config) when is_list(Config) ->
    KillAt = {mnesia2_dumper, post_dump},
    interrupted_create(Config, disc_only_copies, all, KillAt).

interrupted_after_create_nostore(doc) -> [""];
interrupted_after_create_nostore(suite) -> [];
interrupted_after_create_nostore(Config) when is_list(Config) ->
    KillAt = {mnesia2_dumper, post_dump},
    interrupted_create(Config, ram_copies, one, KillAt).

%%% After dump don't need debug point
interrupted_create(Config, Type, _Where, {mnesia2_dumper, post_dump}) ->
    [Node1] = Nodes = ?acquire_nodes(1, [{tc_timeout, timer:seconds(30)} | Config]),
    ?match({atomic, ok},mnesia2:create_table(itrpt, [{Type, Nodes}])),
    ?match({atomic, ok},mnesia2:create_table(test, [{disc_copies,[Node1]}])),
    ?match(ok, mnesia2:dirty_write({itrpt, before, 1})),
    ?match(ok, mnesia2:dirty_write({test, found_in_log, 1})),
    ?match(stopped, mnesia2:stop()),
    ?match([], mnesia2_test_lib:start_mnesia2([Node1], [itrpt,test])),  
    %% Verify 
    ?match([{test, found_in_log, 1}], mnesia2:dirty_read({test, found_in_log})),
    case Type of
	ram_copies -> 	      
	    ?match([], mnesia2:dirty_read({itrpt, before}));
	_ ->
	    ?match([{itrpt, before, 1}], mnesia2:dirty_read({itrpt, before}))
    end,
    ?verify_mnesia2(Nodes, []);
interrupted_create(Config, Type, Where, KillAt) ->
    ?is_debug_compiled,
    [Node1, Node2] = Nodes = ?acquire_nodes(2, [{tc_timeout, timer:seconds(30)} | Config]),
    {success, [A]} = ?start_activities([Node2]),
    setup_dbgpoint(KillAt, Node2),

    if       %% CREATE TABLE
	Where == all ->    % tables on both nodes
	    A ! fun() -> mnesia2:create_table(itrpt, [{Type, Nodes}]) end;
	true ->            % no table on the killed node
	    A ! fun() -> mnesia2:create_table(itrpt, [{Type, [Node1]}]) end
    end,
    
    kill_at_debug(),
    ?match([], mnesia2_test_lib:start_mnesia2([Node2], [itrpt])),  
    %% Verify 
    ?match(ok, mnesia2:dirty_write({itrpt, before, 1})),
    verify_tab(Node1, Node2),
    ?verify_mnesia2(Nodes, []).

interrupted_before_delete_ram(doc) -> [""];
interrupted_before_delete_ram(suite) -> [];
interrupted_before_delete_ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_before_delete(Config, ram_copies, Debug_Point).

interrupted_before_delete_disc(doc) -> [""];
interrupted_before_delete_disc(suite) -> [];
interrupted_before_delete_disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_before_delete(Config, disc_copies, Debug_Point).

interrupted_before_delete_do(doc) -> [""];
interrupted_before_delete_do(suite) -> [];
interrupted_before_delete_do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_before_delete(Config, disc_only_copies, Debug_Point).

interrupted_after_delete_ram(doc) -> [""];
interrupted_after_delete_ram(suite) -> [];
interrupted_after_delete_ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_after_delete(Config, ram_copies, Debug_Point).

interrupted_after_delete_disc(doc) -> [""];
interrupted_after_delete_disc(suite) -> [];
interrupted_after_delete_disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_after_delete(Config, disc_copies, Debug_Point).

interrupted_after_delete_do(doc) -> [""];
interrupted_after_delete_do(suite) -> [];
interrupted_after_delete_do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_after_delete(Config, disc_only_copies, Debug_Point).

interrupted_before_delete(Config, Type, KillAt) ->
    ?is_debug_compiled,
    [Node1, Node2] = Nodes = ?acquire_nodes(2, [{tc_timeout, timer:seconds(30)} | Config]),
    Tab = itrpt,
    ?match({atomic, ok}, mnesia2:create_table(Tab, [{Type, [Node2]}])),
    ?match(ok, mnesia2:dirty_write({Tab, before, 1})),
    {_Alive, Kill} = {Node1, Node2},
    {success, [A]} = ?start_activities([Kill]),

    setup_dbgpoint(KillAt, Kill),
    A ! fun() -> mnesia2:delete_table(Tab) end,

    kill_at_debug(),
    ?match([], mnesia2_test_lib:start_mnesia2([Node2], [])),
    Bad = {badrpc, {'EXIT', {aborted,{no_exists, Tab, all}}}},
    Node1TableInfo = rpc:call(Node1, mnesia2, table_info, [Tab, all]),
    Node2TableInfo = rpc:call(Node2, mnesia2, table_info, [Tab, all]),
    ?match([{access_mode,read_write}|_Rest], Node1TableInfo),
    ?match([{access_mode,read_write}|_Rest], Node2TableInfo),
    ?verify_mnesia2(Nodes, []).

interrupted_after_delete(Config, Type, KillAt) ->
    ?is_debug_compiled,
    [Node1, Node2] = Nodes = ?acquire_nodes(2, [{tc_timeout, timer:seconds(30)} | Config]),
    Tab = itrpt,
    ?match({atomic, ok}, mnesia2:create_table(Tab, [{Type, [Node2]}])),
    ?match(ok, mnesia2:dirty_write({Tab, before, 1})),
    {_Alive, Kill} = {Node1, Node2},
    {success, [A]} = ?start_activities([Kill]),

    setup_dbgpoint(KillAt, Kill),
    A ! fun() -> mnesia2:delete_table(Tab) end,

    kill_at_debug(),
    ?match([], mnesia2_test_lib:start_mnesia2([Node2], [])),
    Bad = {badrpc, {'EXIT', {aborted,{no_exists, Tab, all}}}},
    ?match(Bad, rpc:call(Node1, mnesia2, table_info, [Tab, all])),
    ?match(Bad, rpc:call(Node2, mnesia2, table_info, [Tab, all])),
    ?verify_mnesia2(Nodes, []).

interrupted_before_add_ram(doc) -> [""];
interrupted_before_add_ram(suite) -> [];
interrupted_before_add_ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_add(Config, ram_copies, kill_reciever, Debug_Point).

interrupted_before_add_disc(doc) -> [""];
interrupted_before_add_disc(suite) -> [];
interrupted_before_add_disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_add(Config, disc_copies, kill_reciever, Debug_Point).

interrupted_before_add_do(doc) -> [""];
interrupted_before_add_do(suite) -> [];
interrupted_before_add_do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_add(Config, disc_only_copies, kill_reciever, Debug_Point).

interrupted_before_add_kill_copier(doc) -> [""];
interrupted_before_add_kill_copier(suite) -> [];
interrupted_before_add_kill_copier(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_add(Config, ram_copies, kill_copier, Debug_Point).

interrupted_after_add_ram(doc) -> [""];
interrupted_after_add_ram(suite) -> [];
interrupted_after_add_ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_add(Config, ram_copies, kill_reciever, Debug_Point).

interrupted_after_add_disc(doc) -> [""];
interrupted_after_add_disc(suite) -> [];
interrupted_after_add_disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_add(Config, disc_copies, kill_reciever, Debug_Point).

interrupted_after_add_do(doc) -> [""];
interrupted_after_add_do(suite) -> [];
interrupted_after_add_do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_add(Config, disc_only_copies, kill_reciever, Debug_Point).

interrupted_after_add_kill_copier(doc) -> [""];
interrupted_after_add_kill_copier(suite) -> [];
interrupted_after_add_kill_copier(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_add(Config, ram_copies, kill_copier, Debug_Point).

%%% After dump don't need debug point
interrupted_add(Config, Type, _Where, {mnesia2_dumper, post_dump}) ->
    [Node1, Node2] = Nodes =
	?acquire_nodes(2, [{tc_timeout, timer:seconds(30)} | Config]),
    Tab = itrpt,
    ?match({atomic, ok}, mnesia2:create_table(Tab, [{Type, [Node2]}, {local_content,true}])),
    ?match({atomic, ok},mnesia2:create_table(test, [{disc_copies,[Node1]}])),
    ?match({atomic, ok}, mnesia2:add_table_copy(Tab, Node1, Type)),
    ?match(ok, mnesia2:dirty_write({itrpt, before, 1})),
    ?match(ok, mnesia2:dirty_write({test, found_in_log, 1})),
    ?match(stopped, mnesia2:stop()),
    ?match([], mnesia2_test_lib:start_mnesia2([Node1], [itrpt,test])),
    %% Verify 
    ?match([{test, found_in_log, 1}], mnesia2:dirty_read({test, found_in_log})),
    case Type of
	ram_copies -> 	      
	    ?match([], mnesia2:dirty_read({itrpt, before}));
	_ ->
	    ?match([{itrpt, before, 1}], mnesia2:dirty_read({itrpt, before}))
    end,
    ?verify_mnesia2(Nodes, []);
interrupted_add(Config, Type, Who, KillAt) ->
    ?is_debug_compiled,
    [Node1, Node2] = Nodes =
	?acquire_nodes(2, [{tc_timeout, timer:seconds(30)} | Config]),
    {_Alive, Kill} = 
	if Who == kill_reciever -> 
		{Node1, Node2};
	   true -> 
		{Node2, Node1}
	end,    
    {success, [A]} = ?start_activities([Kill]),
    Tab = itrpt,
    ?match({atomic, ok}, mnesia2:create_table(Tab, [{Type, [Node1]}])),
    ?match(ok, mnesia2:dirty_write({Tab, before, 1})),
    
    setup_dbgpoint(KillAt, Kill),
    
    A ! fun() -> mnesia2:add_table_copy(Tab, Node2, Type) end,
    kill_at_debug(),
    ?match([], mnesia2_test_lib:start_mnesia2([Kill], [itrpt])),    
    verify_tab(Node1, Node2),
    ?verify_mnesia2(Nodes, []).

interrupted_before_move_ram(doc) -> [""];
interrupted_before_move_ram(suite) -> [];
interrupted_before_move_ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_move(Config, ram_copies, kill_reciever, Debug_Point).

interrupted_before_move_disc(doc) -> [""];
interrupted_before_move_disc(suite) -> [];
interrupted_before_move_disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_move(Config, disc_copies, kill_reciever, Debug_Point).

interrupted_before_move_do(doc) -> [""];
interrupted_before_move_do(suite) -> [];
interrupted_before_move_do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_move(Config, disc_only_copies, kill_reciever, Debug_Point).

interrupted_before_move_kill_copier(doc) -> [""];
interrupted_before_move_kill_copier(suite) -> [];
interrupted_before_move_kill_copier(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_move(Config, ram_copies, kill_copier, Debug_Point).

interrupted_after_move_ram(doc) -> [""];
interrupted_after_move_ram(suite) -> [];
interrupted_after_move_ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_move(Config, ram_copies, kill_reciever, Debug_Point).

interrupted_after_move_disc(doc) -> [""];
interrupted_after_move_disc(suite) -> [];
interrupted_after_move_disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_move(Config, disc_copies, kill_reciever, Debug_Point).

interrupted_after_move_do(doc) -> [""];
interrupted_after_move_do(suite) -> [];
interrupted_after_move_do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_move(Config, disc_only_copies, kill_reciever, Debug_Point).

interrupted_after_move_kill_copier(doc) -> [""];
interrupted_after_move_kill_copier(suite) -> [];
interrupted_after_move_kill_copier(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_move(Config, ram_copies, kill_copier, Debug_Point).

%%% After dump don't need debug point
interrupted_move(Config, Type, _Where, {mnesia2_dumper, post_dump}) ->
    [Node1, Node2] = Nodes =
	?acquire_nodes(2, [{tc_timeout, timer:seconds(30)} | Config]),
    Tab = itrpt,
    ?match({atomic, ok},mnesia2:create_table(test, [{disc_copies,[Node1]}])),
    ?match({atomic, ok}, mnesia2:create_table(Tab, [{Type, [Node1]}])),
    ?match(ok, mnesia2:dirty_write({itrpt, before, 1})),
    ?match({atomic, ok}, mnesia2:move_table_copy(Tab, Node1, Node2)),
    ?match(ok, mnesia2:dirty_write({itrpt, aFter, 1})),
    ?match(ok, mnesia2:dirty_write({test, found_in_log, 1})),
    ?match(stopped, mnesia2:stop()),
    ?match([], mnesia2_test_lib:start_mnesia2([Node1], [itrpt,test])),
    %% Verify 
    ?match([{test, found_in_log, 1}], mnesia2:dirty_read({test, found_in_log})),
    ?match([{itrpt, before, 1}], mnesia2:dirty_read({itrpt, before})),
    ?match([{itrpt, aFter, 1}], mnesia2:dirty_read({itrpt, aFter})),
    ?verify_mnesia2(Nodes, []);
interrupted_move(Config, Type, Who, KillAt) ->
    ?is_debug_compiled,
    [Node1, Node2] = Nodes = 
	?acquire_nodes(2, [{tc_timeout, timer:seconds(30)} | Config]),
    Tab = itrpt,
    ?match({atomic, ok}, mnesia2:create_table(Tab, [{Type, [Node1]}])),
    ?match(ok, mnesia2:dirty_write({Tab, before, 1})),

    {_Alive, Kill} = 
	if Who == kill_reciever -> 
		if Type == ram_copies -> 
			{atomic, ok} = mnesia2:dump_tables([Tab]);
		   true -> 
			ignore
		end,
		{Node1, Node2};
	   true -> 
		{Node2, Node1}
	end,
    
    {success, [A]} = ?start_activities([Kill]),
    
    setup_dbgpoint(KillAt, Kill),
    A ! fun() -> mnesia2:move_table_copy(Tab, Node1, Node2) end,
    kill_at_debug(),
    ?match([], mnesia2_test_lib:start_mnesia2([Kill], [itrpt])),    
    verify_tab(Node1, Node2),
    ?verify_mnesia2(Nodes, []).

interrupted_before_delcopy_ram(doc) -> [""];
interrupted_before_delcopy_ram(suite) -> [];
interrupted_before_delcopy_ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_delcopy(Config, ram_copies, kill_reciever, Debug_Point).

interrupted_before_delcopy_disc(doc) -> [""];
interrupted_before_delcopy_disc(suite) -> [];
interrupted_before_delcopy_disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_delcopy(Config, disc_copies, kill_reciever, Debug_Point).

interrupted_before_delcopy_do(doc) -> [""];
interrupted_before_delcopy_do(suite) -> [];
interrupted_before_delcopy_do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_delcopy(Config, disc_only_copies, kill_reciever, Debug_Point).

interrupted_before_delcopy_kill_copier(doc) -> [""];
interrupted_before_delcopy_kill_copier(suite) -> [];
interrupted_before_delcopy_kill_copier(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_delcopy(Config, ram_copies, kill_copier, Debug_Point).

interrupted_after_delcopy_ram(doc) -> [""];
interrupted_after_delcopy_ram(suite) -> [];
interrupted_after_delcopy_ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_delcopy(Config, ram_copies, kill_reciever, Debug_Point).

interrupted_after_delcopy_disc(doc) -> [""];
interrupted_after_delcopy_disc(suite) -> [];
interrupted_after_delcopy_disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_delcopy(Config, disc_copies, kill_reciever, Debug_Point).

interrupted_after_delcopy_do(doc) -> [""];
interrupted_after_delcopy_do(suite) -> [];
interrupted_after_delcopy_do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_delcopy(Config, disc_only_copies, kill_reciever, Debug_Point).

interrupted_after_delcopy_kill_copier(doc) -> [""];
interrupted_after_delcopy_kill_copier(suite) -> [];
interrupted_after_delcopy_kill_copier(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_delcopy(Config, ram_copies, kill_copier, Debug_Point).


%%% After dump don't need debug point
interrupted_delcopy(Config, Type, _Where, {mnesia2_dumper, post_dump}) ->
    [Node1, Node2] = Nodes =
	?acquire_nodes(2, [{tc_timeout, timer:seconds(30)} | Config]),
    Tab = itrpt,
    ?match({atomic, ok},mnesia2:create_table(test, [{disc_copies,[Node1]}])),
    ?match({atomic, ok}, mnesia2:create_table(Tab, [{Type, [Node1,Node2]}])),
    ?match({atomic, ok}, mnesia2:del_table_copy(Tab, Node1)),
    ?match(ok, mnesia2:dirty_write({test, found_in_log, 1})),
    ?match(stopped, mnesia2:stop()),
    ?match([], mnesia2_test_lib:start_mnesia2([Node1], [test])),
    %% Verify 
    ?match([{test, found_in_log, 1}], mnesia2:dirty_read({test, found_in_log})),
    ?match([Node2], mnesia2:table_info(itrpt,Type)),
    ?verify_mnesia2(Nodes, []);
interrupted_delcopy(Config, Type, Who, KillAt) ->
    ?is_debug_compiled,
    [Node1, Node2] = Nodes = 
	?acquire_nodes(2, [{tc_timeout, timer:seconds(30)} | Config]),
    Tab = itrpt,
    ?match({atomic, ok}, mnesia2:create_table(Tab, [{Type, [Node1, Node2]}])),
    ?match(ok, mnesia2:dirty_write({Tab, before, 1})),

    {_Alive, Kill} = 
	if Who == kill_reciever -> 
		{Node1, Node2};
	   true -> 
		if 
		    Type == ram_copies -> 
			{atomic, ok} = mnesia2:dump_tables([Tab]);
		    true -> 
			ignore
		end,
		{Node2, Node1}
	end,

    {success, [A]} = ?start_activities([Kill]),
    setup_dbgpoint(KillAt, Kill),
    A ! fun() -> mnesia2:del_table_copy(Tab, Node2) end,
    kill_at_debug(),
    ?match([], mnesia2_test_lib:start_mnesia2([Kill], [itrpt])),    
    verify_tab(Node1, Node2),
    ?verify_mnesia2(Nodes, []).

interrupted_before_addindex_ram(doc) -> [""];
interrupted_before_addindex_ram(suite) -> [];
interrupted_before_addindex_ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_addindex(Config, ram_copies, Debug_Point).

interrupted_before_addindex_disc(doc) -> [""];
interrupted_before_addindex_disc(suite) -> [];
interrupted_before_addindex_disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_addindex(Config, disc_copies, Debug_Point).

interrupted_before_addindex_do(doc) -> [""];
interrupted_before_addindex_do(suite) -> [];
interrupted_before_addindex_do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_addindex(Config, disc_only_copies, Debug_Point).

interrupted_after_addindex_ram(doc) -> [""];
interrupted_after_addindex_ram(suite) -> [];
interrupted_after_addindex_ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},
    interrupted_addindex(Config, ram_copies, Debug_Point).

interrupted_after_addindex_disc(doc) -> [""];
interrupted_after_addindex_disc(suite) -> [];
interrupted_after_addindex_disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_addindex(Config, disc_copies, Debug_Point).

interrupted_after_addindex_do(doc) -> [""];
interrupted_after_addindex_do(suite) -> [];
interrupted_after_addindex_do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_addindex(Config, disc_only_copies, Debug_Point).


%%% After dump don't need debug point
interrupted_addindex(Config, Type, {mnesia2_dumper, post_dump}) ->
    [Node1] = Nodes = ?acquire_nodes(1, [{tc_timeout, timer:seconds(30)} | Config]),
    Tab = itrpt,
    ?match({atomic,ok},mnesia2:create_table(Tab, [{Type, Nodes}])),
    ?match({atomic,ok},mnesia2:create_table(test, [{disc_copies,[Node1]}])),
    ?match({atomic,ok}, mnesia2:add_table_index(Tab, val)),
    ?match(ok, mnesia2:dirty_write({itrpt, before, 1})),
    ?match(ok, mnesia2:dirty_write({test, found_in_log, 1})),
    ?match(stopped, mnesia2:stop()),
    ?match([], mnesia2_test_lib:start_mnesia2([Node1], [itrpt,test])),  
    %% Verify 
    ?match([{test, found_in_log, 1}], mnesia2:dirty_read({test, found_in_log})),
    case Type of
	ram_copies -> 	      
	    ?match([], mnesia2:dirty_index_read(itrpt, 1, val));
	_ ->
	    ?match([{itrpt, before, 1}], mnesia2:dirty_index_read(itrpt, 1, val))
    end,
    ?verify_mnesia2(Nodes, []);
interrupted_addindex(Config, Type, KillAt) ->
    ?is_debug_compiled,
    [Node1, Node2] = Nodes = ?acquire_nodes(2, [{tc_timeout, timer:seconds(30)} | Config]),
    Tab = itrpt,
    ?match({atomic, ok}, mnesia2:create_table(Tab, [{Type, [Node1]}])),
    ?match(ok, mnesia2:dirty_write({Tab, before, 1})),
    {_Alive, Kill} = {Node1, Node2},
    {success, [A]} = ?start_activities([Kill]),
    
    setup_dbgpoint(KillAt, Kill),
    A ! fun() -> mnesia2:add_table_index(Tab, val) end,
    kill_at_debug(),
    ?match([], mnesia2_test_lib:start_mnesia2([Node2], [])),

    verify_tab(Node1, Node2),
    ?match([{Tab, b, a}, {Tab, a, a}], 
	   rpc:call(Node1, mnesia2, dirty_index_read, [itrpt, a, val])),
    ?match([{Tab, b, a}, {Tab, a, a}], 
	   rpc:call(Node2, mnesia2, dirty_index_read, [itrpt, a, val])),
    ?verify_mnesia2(Nodes, []).

interrupted_before_delindex_ram(doc) -> [""];
interrupted_before_delindex_ram(suite) -> [];
interrupted_before_delindex_ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_delindex(Config, ram_copies, Debug_Point).

interrupted_before_delindex_disc(doc) -> [""];
interrupted_before_delindex_disc(suite) -> [];
interrupted_before_delindex_disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_delindex(Config, disc_copies, Debug_Point).

interrupted_before_delindex_do(doc) -> [""];
interrupted_before_delindex_do(suite) -> [];
interrupted_before_delindex_do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_delindex(Config, disc_only_copies, Debug_Point).

interrupted_after_delindex_ram(doc) -> [""];
interrupted_after_delindex_ram(suite) -> [];
interrupted_after_delindex_ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_delindex(Config, ram_copies, Debug_Point).

interrupted_after_delindex_disc(doc) -> [""];
interrupted_after_delindex_disc(suite) -> [];
interrupted_after_delindex_disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_delindex(Config, disc_copies, Debug_Point).

interrupted_after_delindex_do(doc) -> [""];
interrupted_after_delindex_do(suite) -> [];
interrupted_after_delindex_do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_delindex(Config, disc_only_copies, Debug_Point).

%%% After dump don't need debug point
interrupted_delindex(Config, Type, {mnesia2_dumper, post_dump}) ->
    [Node1] = Nodes = ?acquire_nodes(1, [{tc_timeout, timer:seconds(30)} | Config]),
    Tab = itrpt,
    ?match({atomic,ok},mnesia2:create_table(Tab, [{Type, Nodes},{index,[val]}])),
    ?match({atomic,ok},mnesia2:create_table(test, [{disc_copies,[Node1]}])),
    ?match({atomic,ok}, mnesia2:del_table_index(Tab, val)),
    ?match(ok, mnesia2:dirty_write({itrpt, before, 1})),
    ?match(ok, mnesia2:dirty_write({test, found_in_log, 1})),
    ?match(stopped, mnesia2:stop()),
    ?match([], mnesia2_test_lib:start_mnesia2([Node1], [itrpt,test])),  
    %% Verify 
    ?match([{test, found_in_log, 1}], mnesia2:dirty_read({test, found_in_log})),
    ?match({'EXIT',{aborted,{badarg,_}}}, mnesia2:dirty_index_read(itrpt, 1, val)),
    ?verify_mnesia2(Nodes, []);

interrupted_delindex(Config, Type, KillAt) ->
    ?is_debug_compiled,
    [Node1, Node2] = Nodes = ?acquire_nodes(2, [{tc_timeout, timer:seconds(30)} | Config]),
    Tab = itrpt,
    ?match({atomic, ok}, mnesia2:create_table(Tab, [{index, [val]}, 
						   {Type, [Node1]}])),
    ?match(ok, mnesia2:dirty_write({Tab, before, 1})),
    {_Alive, Kill} = {Node1, Node2},
    {success, [A]} = ?start_activities([Kill]),
    setup_dbgpoint(KillAt, Kill),
    A ! fun() -> mnesia2:del_table_index(Tab, val) end,
    kill_at_debug(),
    ?match([], mnesia2_test_lib:start_mnesia2([Node2], [])),  
    verify_tab(Node1, Node2),
    ?match({badrpc, _}, rpc:call(Node1, mnesia2, dirty_index_read, [itrpt, a, val])),
    ?match({badrpc, _}, rpc:call(Node2, mnesia2, dirty_index_read, [itrpt, a, val])),
    ?match([], rpc:call(Node1, mnesia2, table_info, [Tab, index])),
    ?match([], rpc:call(Node2, mnesia2, table_info, [Tab, index])),
    ?verify_mnesia2(Nodes, []).

interrupted_before_change_type_ram2disc(doc) -> [""];
interrupted_before_change_type_ram2disc(suite) -> [];
interrupted_before_change_type_ram2disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_change_type(Config, ram_copies, disc_copies, changer, Debug_Point).

interrupted_before_change_type_ram2do(doc) -> [""];
interrupted_before_change_type_ram2do(suite) -> [];
interrupted_before_change_type_ram2do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_change_type(Config, ram_copies, disc_only_copies, changer, Debug_Point).    

interrupted_before_change_type_disc2ram(doc) -> [""];
interrupted_before_change_type_disc2ram(suite) -> [];
interrupted_before_change_type_disc2ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_change_type(Config, disc_copies, ram_copies, changer, Debug_Point).

interrupted_before_change_type_disc2do(doc) -> [""];
interrupted_before_change_type_disc2do(suite) -> [];
interrupted_before_change_type_disc2do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_change_type(Config, disc_copies, disc_only_copies, changer, Debug_Point).

interrupted_before_change_type_do2ram(doc) -> [""];
interrupted_before_change_type_do2ram(suite) -> [];
interrupted_before_change_type_do2ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_change_type(Config, disc_only_copies, ram_copies, changer, Debug_Point).

interrupted_before_change_type_do2disc(doc) -> [""];
interrupted_before_change_type_do2disc(suite) -> [];
interrupted_before_change_type_do2disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_change_type(Config, disc_only_copies, disc_copies, changer, Debug_Point).

interrupted_before_change_type_other_node(doc) -> [""];
interrupted_before_change_type_other_node(suite) -> [];
interrupted_before_change_type_other_node(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, dump_schema_op},    
    interrupted_change_type(Config, ram_copies, disc_copies, the_other_one, Debug_Point).

interrupted_after_change_type_ram2disc(doc) -> [""];
interrupted_after_change_type_ram2disc(suite) -> [];
interrupted_after_change_type_ram2disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_change_type(Config, ram_copies, disc_copies, changer, Debug_Point).

interrupted_after_change_type_ram2do(doc) -> [""];
interrupted_after_change_type_ram2do(suite) -> [];
interrupted_after_change_type_ram2do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_change_type(Config, ram_copies, disc_only_copies, changer, Debug_Point).    

interrupted_after_change_type_disc2ram(doc) -> [""];
interrupted_after_change_type_disc2ram(suite) -> [];
interrupted_after_change_type_disc2ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_change_type(Config, disc_copies, ram_copies, changer, Debug_Point).

interrupted_after_change_type_disc2do(doc) -> [""];
interrupted_after_change_type_disc2do(suite) -> [];
interrupted_after_change_type_disc2do(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_change_type(Config, disc_copies, disc_only_copies, changer, Debug_Point).

interrupted_after_change_type_do2ram(doc) -> [""];
interrupted_after_change_type_do2ram(suite) -> [];
interrupted_after_change_type_do2ram(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_change_type(Config, disc_only_copies, ram_copies, changer, Debug_Point).

interrupted_after_change_type_do2disc(doc) -> [""];
interrupted_after_change_type_do2disc(suite) -> [];
interrupted_after_change_type_do2disc(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_change_type(Config, disc_only_copies, disc_copies, changer, Debug_Point).

interrupted_after_change_type_other_node(doc) -> [""];
interrupted_after_change_type_other_node(suite) -> [];
interrupted_after_change_type_other_node(Config) when is_list(Config) ->
    Debug_Point = {mnesia2_dumper, post_dump},    
    interrupted_change_type(Config, ram_copies, disc_copies, the_other_one, Debug_Point).

interrupted_change_type(Config, FromType, ToType, Who, KillAt) ->
    ?is_debug_compiled,
    [Node1, Node2] = Nodes = ?acquire_nodes(2, [{tc_timeout, timer:seconds(30)} | Config]),
    Tab = itrpt,
    ?match({atomic, ok}, mnesia2:create_table(Tab, [{FromType, [Node2, Node1]}])),
    ?match(ok, mnesia2:dirty_write({Tab, before, 1})),

    {_Alive, Kill} = 
	if Who == changer -> {Node1, Node2};
	   true ->           {Node2, Node1}
	end,
    
    {success, [A]} = ?start_activities([Kill]),
    setup_dbgpoint(KillAt, Kill),
    A ! fun() -> mnesia2:change_table_copy_type(Tab, Node2, ToType) end,
    kill_at_debug(),
    ?match([], mnesia2_test_lib:start_mnesia2(Nodes, [itrpt])),        
    verify_tab(Node1, Node2),
    ?match(FromType, rpc:call(Node1, mnesia2, table_info, [Tab, storage_type])),
    ?match(ToType, rpc:call(Node2, mnesia2, table_info, [Tab, storage_type])),
    ?verify_mnesia2(Nodes, []).

interrupted_before_change_schema_type(doc) -> [""];
interrupted_before_change_schema_type(suite) ->     [];
interrupted_before_change_schema_type(Config) when is_list(Config) ->
    KillAt = {mnesia2_dumper, dump_schema_op},
    interrupted_change_schema_type(Config, KillAt).

interrupted_after_change_schema_type(doc) -> [""];
interrupted_after_change_schema_type(suite) ->     [];
interrupted_after_change_schema_type(Config) when is_list(Config) ->
    KillAt = {mnesia2_dumper, post_dump},
    interrupted_change_schema_type(Config, KillAt).

-define(cleanup(N, Config),
	mnesia2_test_lib:prepare_test_case([{reload_appls, [mnesia2]}],
					  N, Config, ?FILE, ?LINE)).
    
interrupted_change_schema_type(Config, KillAt) ->
    ?is_debug_compiled,
    [Node1, Node2] = Nodes = ?acquire_nodes(2, [{tc_timeout, timer:seconds(30)} | Config]),

    Tab = itrpt,
    ?match({atomic, ok}, mnesia2:create_table(Tab, [{ram_copies, [Node2, Node1]}])),
    ?match(ok, mnesia2:dirty_write({Tab, before, 1})),
    
    {success, [A]} = ?start_activities([Node2]),
    setup_dbgpoint(KillAt, Node2),	
    
    A ! fun() -> mnesia2:change_table_copy_type(schema, Node2, ram_copies) end,
    kill_at_debug(),    
    ?match(ok, rpc:call(Node2, mnesia2, start, [[{extra_db_nodes, [Node1, Node2]}]])),
    ?match(ok, rpc:call(Node2, mnesia2, wait_for_tables, [[itrpt, schema], 2000])),
    ?match(disc_copies, rpc:call(Node1, mnesia2, table_info, [schema, storage_type])),
    ?match(ram_copies, rpc:call(Node2, mnesia2, table_info,  [schema, storage_type])),
    
    %% Go back to disc_copies !!    
    {success, [B]} = ?start_activities([Node2]),
    setup_dbgpoint(KillAt, Node2),	
    B ! fun() -> mnesia2:change_table_copy_type(schema, Node2, disc_copies) end,
    kill_at_debug(),

    ?match(ok, rpc:call(Node2, mnesia2, start, [[{extra_db_nodes, [Node1, Node2]}]])),
    ?match(ok, rpc:call(Node2, mnesia2, wait_for_tables, [[itrpt, schema], 2000])),
    ?match(disc_copies, rpc:call(Node1, mnesia2, table_info, [schema, storage_type])),
    ?match(disc_copies, rpc:call(Node2, mnesia2, table_info,  [schema, storage_type])),

    ?verify_mnesia2(Nodes, []),
    ?cleanup(2, Config).

%%% Helpers
verify_tab(Node1, Node2) ->
    ?match({atomic, ok}, 
	   rpc:call(Node1, mnesia2, transaction, [fun() -> mnesia2:dirty_write({itrpt, a, a}) end])),
    ?match({atomic, ok},
	   rpc:call(Node2, mnesia2, transaction, [fun() -> mnesia2:dirty_write({itrpt, b, a}) end])),
    ?match([{itrpt,a,a}], rpc:call(Node1, mnesia2, dirty_read, [{itrpt, a}])), 
    ?match([{itrpt,a,a}], rpc:call(Node2, mnesia2, dirty_read, [{itrpt, a}])),
    ?match([{itrpt,b,a}], rpc:call(Node1, mnesia2, dirty_read, [{itrpt, b}])), 
    ?match([{itrpt,b,a}], rpc:call(Node2, mnesia2, dirty_read, [{itrpt, b}])),
    ?match([{itrpt,before,1}], rpc:call(Node1, mnesia2, dirty_read, [{itrpt, before}])),
    ?match([{itrpt,before,1}], rpc:call(Node2, mnesia2, dirty_read, [{itrpt, before}])).

setup_dbgpoint(DbgPoint, Where) -> 
    Self = self(),
    TestFun = fun(_, [InitBy]) ->
		      case InitBy of
			  schema_prepare ->
			      ignore;
			  schema_begin ->
			      ignore;
			  _Other ->
			      ?deactivate_debug_fun(DbgPoint),
			      unlink(Self),
			      Self ! {fun_done, node()},
			      timer:sleep(infinity)
		      end
	      end, 
    %% Kill when debug has been reached
    ?remote_activate_debug_fun(Where, DbgPoint, TestFun, []).

kill_at_debug() ->   
    %% Wait till it's killed
    receive 
	{fun_done, Node} -> 
	    ?match([], mnesia2_test_lib:kill_mnesia2([Node]));
  {Pid, {atomic,ok}} ->
      ?match([], mnesia2_test_lib:kill_mnesia2([node(Pid)]))
    after 
	timer:minutes(1) -> ?error("Timeout in kill_at_debug", [])
    end.

