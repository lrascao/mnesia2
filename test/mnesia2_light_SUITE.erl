%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1997-2013. All Rights Reserved.
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
-module(mnesia2_light_SUITE).
-author('hakan@erix.ericsson.se').
-compile([export_all]).
-include_lib("common_test/include/ct.hrl").
-include("mnesia2_test_lib.hrl").

init_per_testcase(Func, Conf) ->
    mnesia2_test_lib:init_per_testcase(Func, Conf).

end_per_testcase(Func, Conf) ->
    mnesia2_test_lib:end_per_testcase(Func, Conf).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
suite() -> [{ct_hooks,[{ts_install_cth,[{nodenames,2}]}]}].

%% Verify that Mnesia2 really is a distributed real-time DBMS.
%% This is the test suite of the Mnesia2 DBMS. The test suite
%% covers many aspects of usage and is indended to be developed
%% incrementally. The test suite is divided into a hierarchy of test
%% suites where the leafs actually implements the test cases.
%% The intention of each test case and sub test suite can be
%% read in comments where they are implemented or in worst cases
%% from their long mnemonic names.
%%
%% The most simple test case of them all is called 'silly'
%% and is useful to run now and then, e.g. when some new fatal
%% bug has been introduced. It may be run even if Mnesia2 is in
%% such a bad shape that the test machinery cannot be used.
%% NB! Invoke the function directly with mnesia2_SUITE:silly()
%% and do not involve the normal test machinery.

all() ->
    [{group, light}, clean_up_suite].

groups() ->
    %% The 'light' test suite runs a selected set of test suites and is
    %% intended to be the smallest test suite that is meaningful
    %% to run. It starts with an installation test (which in essence is the
    %% 'silly' test case) and then it covers all functions in the API in
    %% various depths. All configuration parameters and examples are also
    %% covered.
    [{light, [],
      [{group, install}, {group, nice}, {group, evil},
       {group, mnesia2_frag_test, light}, {group, qlc},
       {group, registry}, {group, config}, {group, examples}]},
     {install, [], [{mnesia2_install_test, all}]},
     {nice, [], [{mnesia2_nice_coverage_test, all}]},
     {evil, [], [{mnesia2_evil_coverage_test, all}]},
     {qlc, [], [{mnesia2_qlc_test, all}]},
     {registry, [], [{mnesia2_registry_test, all}]},
     {config, [], [{mnesia2_config_test, all}]},
     {examples, [], [{mnesia2_examples_test, all}]}].

init_per_group(GroupName, Config) ->
  ct:print(default, 50, "starting test group: ~p", [GroupName]),
	Config.

end_per_group(GroupName, Config) ->
  ct:print(default, 50, "ending test group: ~p", [GroupName]),
	Config.

init_per_suite(Config) ->
    Config.

end_per_suite(Config) ->
    Config.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

clean_up_suite(doc) -> ["Not a test case only kills mnesia2 and nodes, that were"
			"started during the tests"];
clean_up_suite(suite) ->
    [];
clean_up_suite(Config) when is_list(Config)->
    mnesia2:kill(),
    Slaves = mnesia2_test_lib:lookup_config(nodenames, Config),
    Nodes = lists:delete(node(), Slaves),
    rpc:multicall(Nodes, erlang, halt, []),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
