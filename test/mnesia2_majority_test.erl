%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2010-2011. All Rights Reserved.
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
-module(mnesia2_majority_test).
-author('ulf.wiger@erlang-solutions.com').
-compile(export_all).
-include("mnesia2_test_lib.hrl").

init_per_testcase(Func, Conf) ->
    mnesia2_test_lib:init_per_testcase(Func, Conf).

end_per_testcase(Func, Conf) ->
    mnesia2_test_lib:end_per_testcase(Func, Conf).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
all() ->
    [
     write
     , wread
     , delete
     , clear_table
     , frag
     , change_majority
     , frag_change_majority
    ].

write(suite) -> [];
write(Config) when is_list(Config) ->
    [N1, N2, N3] = ?acquire_nodes(3, Config),
    Tab = t,
    Schema = [{name, Tab}, {ram_copies, [N1,N2,N3]}, {majority,true}],
    ?match({atomic, ok}, mnesia2:create_table(Schema)),
    ?match({[ok,ok,ok],[]},
	   rpc:multicall([N1,N2,N3], mnesia2, wait_for_tables, [[Tab], 3000])),
    ?match({atomic,ok},
	   mnesia2:transaction(fun() -> mnesia2:write({t,1,a}) end)),
    mnesia2_test_lib:kill_mnesia2([N3]),
    ?match({atomic,ok},
	   mnesia2:transaction(fun() -> mnesia2:write({t,1,a}) end)),
    mnesia2_test_lib:kill_mnesia2([N2]),
    ?match({aborted,{no_majority,Tab}},
	   mnesia2:transaction(fun() -> mnesia2:write({t,1,a}) end)).

wread(suite) -> [];
wread(Config) when is_list(Config) ->
    [N1, N2] = ?acquire_nodes(2, Config),
    Tab = t,
    Schema = [{name, Tab}, {ram_copies, [N1,N2]}, {majority,true}],
    ?match({atomic, ok}, mnesia2:create_table(Schema)),
    ?match({[ok,ok],[]},
	   rpc:multicall([N1,N2], mnesia2, wait_for_tables, [[Tab], 3000])),
    ?match({atomic,[]},
	   mnesia2:transaction(fun() -> mnesia2:read(t,1,write) end)),
    mnesia2_test_lib:kill_mnesia2([N2]),
    ?match({aborted,{no_majority,Tab}},
	   mnesia2:transaction(fun() -> mnesia2:read(t,1,write) end)).

delete(suite) -> [];
delete(Config) when is_list(Config) ->
    [N1, N2] = ?acquire_nodes(2, Config),
    Tab = t,
    Schema = [{name, Tab}, {ram_copies, [N1,N2]}, {majority,true}],
    ?match({atomic, ok}, mnesia2:create_table(Schema)),
    ?match({[ok,ok],[]},
	   rpc:multicall([N1,N2], mnesia2, wait_for_tables, [[Tab], 3000])),
    %% works as expected with majority of nodes present
    ?match({atomic,ok},
	   mnesia2:transaction(fun() -> mnesia2:write({t,1,a}) end)),
    ?match({atomic,ok},
	   mnesia2:transaction(fun() -> mnesia2:delete({t,1}) end)),
    ?match({atomic,[]},
	   mnesia2:transaction(fun() -> mnesia2:read({t,1}) end)),
    %% put the record back
    ?match({atomic,ok},
	   mnesia2:transaction(fun() -> mnesia2:write({t,1,a}) end)),
    ?match({atomic,[{t,1,a}]},
	   mnesia2:transaction(fun() -> mnesia2:read({t,1}) end)),
    mnesia2_test_lib:kill_mnesia2([N2]),
    ?match({aborted,{no_majority,Tab}},
	   mnesia2:transaction(fun() -> mnesia2:delete({t,1}) end)).

clear_table(suite) -> [];
clear_table(Config) when is_list(Config) ->
    [N1, N2] = ?acquire_nodes(2, Config),
    Tab = t,
    Schema = [{name, Tab}, {ram_copies, [N1,N2]}, {majority,true}],
    ?match({atomic, ok}, mnesia2:create_table(Schema)),
    ?match({[ok,ok],[]},
	   rpc:multicall([N1,N2], mnesia2, wait_for_tables, [[Tab], 3000])),
    %% works as expected with majority of nodes present
    ?match({atomic,ok},
	   mnesia2:transaction(fun() -> mnesia2:write({t,1,a}) end)),
    ?match({atomic,ok}, mnesia2:clear_table(t)),
    ?match({atomic,[]},
	   mnesia2:transaction(fun() -> mnesia2:read({t,1}) end)),
    %% put the record back
    ?match({atomic,ok},
	   mnesia2:transaction(fun() -> mnesia2:write({t,1,a}) end)),
    ?match({atomic,[{t,1,a}]},
	   mnesia2:transaction(fun() -> mnesia2:read({t,1}) end)),
    mnesia2_test_lib:kill_mnesia2([N2]),
    ?match({aborted,{no_majority,Tab}}, mnesia2:clear_table(t)).

frag(suite) -> [];
frag(Config) when is_list(Config) ->
    [N1] = ?acquire_nodes(1, Config),
    Tab = t,
    Schema = [
	      {name, Tab}, {ram_copies, [N1]},
	      {majority,true},
	      {frag_properties, [{n_fragments, 2}]}
	     ],
    ?match({atomic, ok}, mnesia2:create_table(Schema)),
    ?match(true, mnesia2:table_info(t, majority)),
    ?match(true, mnesia2:table_info(t_frag2, majority)).

change_majority(suite) -> [];
change_majority(Config) when is_list(Config) ->
    [N1,N2] = ?acquire_nodes(2, Config),
    Tab = t,
    Schema = [
	      {name, Tab}, {ram_copies, [N1,N2]},
	      {majority,false}
	     ],
    ?match({atomic, ok}, mnesia2:create_table(Schema)),
    ?match(false, mnesia2:table_info(t, majority)),
    ?match({atomic, ok},
	   mnesia2:change_table_majority(t, true)),
    ?match(true, mnesia2:table_info(t, majority)),
    ?match(ok,
	   mnesia2:activity(transaction, fun() ->
						mnesia2:write({t,1,a})
					end)),
    mnesia2_test_lib:kill_mnesia2([N2]),
    ?match({'EXIT',{aborted,{no_majority,_}}},
	   mnesia2:activity(transaction, fun() ->
						mnesia2:write({t,1,a})
					end)).

frag_change_majority(suite) -> [];
frag_change_majority(Config) when is_list(Config) ->
    [N1,N2] = ?acquire_nodes(2, Config),
    Tab = t,
    Schema = [
	      {name, Tab}, {ram_copies, [N1,N2]},
	      {majority,false},
	      {frag_properties,
	       [{n_fragments, 2},
		{n_ram_copies, 2},
		{node_pool, [N1,N2]}]}
	     ],
    ?match({atomic, ok}, mnesia2:create_table(Schema)),
    ?match(false, mnesia2:table_info(t, majority)),
    ?match(false, mnesia2:table_info(t_frag2, majority)),
    ?match({aborted,{bad_type,t_frag2}},
	   mnesia2:change_table_majority(t_frag2, true)),
    ?match({atomic, ok},
	   mnesia2:change_table_majority(t, true)),
    ?match(true, mnesia2:table_info(t, majority)),
    ?match(true, mnesia2:table_info(t_frag2, majority)),
    ?match(ok,
	   mnesia2:activity(transaction, fun() ->
						mnesia2:write({t,1,a})
					end, mnesia2_frag)),
    mnesia2_test_lib:kill_mnesia2([N2]),
    ?match({'EXIT',{aborted,{no_majority,_}}},
	   mnesia2:activity(transaction, fun() ->
						mnesia2:write({t,1,a})
					end, mnesia2_frag)).
