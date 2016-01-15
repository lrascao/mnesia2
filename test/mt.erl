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
%%% Author: Hakan Mattsson  hakan@erix.ericsson.se
%%% Purpose: Nice shortcuts intended for testing of Mnesia
%%%
%%% See the mnesia_SUITE module about the structure of
%%% the test suite.
%%%
%%% See the mnesia2_test_lib module about the test case execution.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

-module(mt).
-author('hakan@erix.ericsson.se').
-export([
	 t/0, t/1, t/2, t/3,                % Run test cases
	 loop/1, loop/2, loop/3,            % loop test cases
	 doc/0, doc/1,                      % Generate test case doc
	 struct/0, struct/1,                % View test suite struct
	 shutdown/0, ping/0, start_nodes/0, % Node admin
	 read_config/0, write_config/1      % Config admin
	]).

-include("mnesia2_test_lib.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Aliases for the (sub) test suites
alias(all) -> mnesia2_SUITE;
alias(atomicity) -> mnesia2_atomicity_test;
alias(backup) -> mnesia2_evil_backup;
alias(config) -> mnesia2_config_test;
alias(consistency) -> mnesia2_consistency_test;
alias(dirty) -> mnesia2_dirty_access_test;
alias(durability) -> mnesia2_durability_test;
alias(evil) -> mnesia2_evil_coverage_test;
alias(qlc) -> mnesia2_qlc_test;
alias(examples) -> mnesia2_examples_test;
alias(frag) -> mnesia2_frag_test;
alias(heavy) -> {mnesia2_SUITE, heavy};
alias(install) -> mnesia2_install_test;
alias(isolation) -> mnesia2_isolation_test;
alias(light) -> {mnesia2_SUITE, light};
alias(majority) -> mnesia2_majority_test;
alias(measure) -> mnesia2_measure_test;
alias(medium) -> {mnesia2_SUITE, medium};
alias(nice) -> mnesia2_nice_coverage_test;
alias(recover) -> mnesia2_recover_test;
alias(recovery) -> mnesia2_recovery_test;
alias(registry) -> mnesia2_registry_test;
alias(suite) -> mnesia2_SUITE;
alias(trans) -> mnesia2_trans_access_test;
alias(Other) -> Other.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Resolves the name of test suites and test cases
%% according to the alias definitions. Single atoms
%% are assumed to be the name of a test suite. 
resolve(Suite0) when is_atom(Suite0) ->
    case alias(Suite0) of
	Suite when is_atom(Suite) ->
	    {Suite, all};
	{Suite, Case} ->
	    {Suite, is_group(Suite,Case)}
    end;
resolve({Suite0, Case}) when is_atom(Suite0), is_atom(Case) ->
    case alias(Suite0) of
	Suite when is_atom(Suite) ->
	    {Suite, is_group(Suite,Case)};
	{Suite, Case2} ->
	    {Suite, is_group(Suite,Case2)}
    end;
resolve(List) when is_list(List) ->
    [resolve(Case) || Case <- List].

is_group(Mod, Case) ->
    try {_,_,_} = lists:keyfind(Case, 1, Mod:groups()),
	 {group, Case}
    catch _:{badmatch,_} ->
	    Case
    end.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Run one or more test cases

%% Run the default test case with default config
t() ->
    t(read_test_case()).

%% Resolve the test case name and run the test case
%% The test case is noted as default test case
%% and the outcome of the tests are written to
%% to a file.
t(silly) ->
    mnesia2_install_test:silly();
t(diskless) ->
    %% Run the default test case with default config,
    %% but diskless
    t(read_test_case(), diskless);
t(Case) ->
    %% Use the default config
    t(Case, read_config()).

t(Case, Config) when Config == diskless ->
    %% Run the test case with default config, but diskless
    Config2 = [{diskless, true} | read_config()],
    t(Case, Config2);
t(Mod, Fun) when is_atom(Mod), is_atom(Fun) ->
    %% Run the test case with default config
    t({Mod, Fun}, read_config());
t(RawCase, Config) when is_list(Config) ->
    %% Resolve the test case name and run the test case
    Case = resolve(RawCase),
    write_test_case(Case),
    Res = mnesia2_test_lib:test(Case, Config),
    append_test_case_info(Case, Res).

t(Mod, Fun, Config) when Config == diskless ->
    t({Mod, Fun}, diskless).

config_fname() ->
    "mnesia2_test_case_config".

%% Read default config file
read_config() ->
    Fname = config_fname(),
    mnesia2_test_lib:log("Consulting file ~s...~n", [Fname]),
    case file:consult(Fname) of
        {ok, Config} ->
	    mnesia2_test_lib:log("Read config ~w~n", [Config]),
            Config;
        _Error ->
	    Config = mnesia2_test_lib:default_config(),
            mnesia2_test_lib:log("<>WARNING<> Using default config: ~w~n", [Config]),
            Config
    end.

%% Write new default config file
write_config(Config) when is_list(Config) ->
    Fname = config_fname(),
    {ok, Fd} = file:open(Fname, write),
    write_list(Fd, Config),
    file:close(Fd).
	    
write_list(Fd, [H | T]) ->
    ok = io:format(Fd, "~p.~n",[H]),
    write_list(Fd, T);
write_list(_, []) ->
    ok.

test_case_fname() ->
    "mnesia2_test_case_info".

%% Read name of test case
read_test_case() ->
    Fname = test_case_fname(),
    case file:open(Fname, [read]) of
	{ok, Fd} ->
	    Res = io:read(Fd, []),
	    file:close(Fd),
	    case Res of
		{ok, TestCase} ->
		    mnesia2_test_lib:log("Using test case ~w from file ~s~n",
					[TestCase, Fname]),
		    TestCase;
		{error, _} ->
		    default_test_case(Fname)
	    end;
	{error, _} ->
	    default_test_case(Fname)
    end.

default_test_case(Fname) ->
    TestCase = all, 
    mnesia2_test_lib:log("<>WARNING<> Cannot read file ~s, "
			"using default test case: ~w~n",
			[Fname, TestCase]),
    TestCase.

write_test_case(TestCase) ->
    Fname = test_case_fname(),
    {ok, Fd} = file:open(Fname, write),
    ok = io:format(Fd, "~p.~n",[TestCase]),
    file:close(Fd).
	    
append_test_case_info(TestCase, TestCaseInfo) ->
    Fname = test_case_fname(),
    {ok, Fd} = file:open(Fname, [read, write]),
    ok = io:format(Fd, "~p.~n",[TestCase]),
    ok = io:format(Fd, "~p.~n",[TestCaseInfo]),
    file:close(Fd),
    TestCaseInfo.
	    
%% Generate HTML pages from the test case structure
doc() ->
    doc(suite).

doc(Case) ->
    mnesia2_test_lib:doc(resolve(Case)).

%% Display out the test case structure
struct() ->
    struct(suite).

struct(Case) ->
    mnesia2_test_lib:struct([resolve(Case)]).

%% Shutdown all nodes with erlang:halt/0
shutdown() ->
    mnesia2_test_lib:shutdown().

%% Ping all nodes in config spec
ping() ->
    Config = read_config(),
    Nodes = mnesia2_test_lib:select_nodes(all, Config, ?FILE, ?LINE),
    [{N, net_adm:ping(N)} || N <- Nodes].

%% Slave start all nodes in config spec
start_nodes() ->
    Config = read_config(),
    Nodes = mnesia2_test_lib:select_nodes(all, Config, ?FILE, ?LINE),
    mnesia2_test_lib:init_nodes(Nodes, ?FILE, ?LINE),
    ping().

%% loop one testcase /suite until it fails

loop(Case) ->
    loop_1(Case,-1,read_config()).

loop(M,F) when is_atom(F) ->
    loop_1({M,F},-1,read_config());
loop(Case,N) when is_integer(N) ->
    loop_1(Case, N,read_config()).

loop(M,F,N) when is_integer(N) ->
    loop_1({M,F},N,read_config()).

loop_1(Case,N,Config) when N /= 0 ->
    io:format("Loop test ~p ~n", [abs(N)]),
    case ok_result(Res = t(Case,Config)) of
	true ->
	    loop_1(Case,N-1,Config);
	error ->
	    Res
    end;
loop_1(_,_,_) ->
    ok.
	    
ok_result([{_T,{ok,_,_}}|R]) -> 
    ok_result(R);
ok_result([{_T,{TC,List}}|R]) when is_tuple(TC), is_list(List) -> 
    ok_result(List) andalso ok_result(R);
ok_result([]) -> true;
ok_result(_) -> error.
