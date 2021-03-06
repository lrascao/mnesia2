%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2011. All Rights Reserved.
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
-define(log(Format,Args),mnesia2_test_lib:log(Format,Args,?FILE,?LINE)).
-define(warning(Format,Args),?log("<>WARNING<>~n " ++ Format,Args)).
-define(error(Format,Args),
	mnesia2_test_lib:error(Format,Args,?FILE,?LINE)).
-define(verbose(Format,Args),mnesia2_test_lib:verbose(Format,Args,?FILE,?LINE)).

-define(fatal(Format,Args),
	?error(Format, Args),
	exit({test_case_fatal, Format, Args, ?FILE, ?LINE})).

-define(skip(Format,Args),
	?warning(Format, Args),
	exit({skipped, ?flat_format(Format, Args)})).

-define(flat_format(Format,Args),
        lists:flatten(io_lib:format(Format, Args))).

-define(sort(What), mnesia2_test_lib:sort(What)).

-define(ignore(Expr),
	fun() ->
		AcTuAlReS = (catch (Expr)),
		?verbose("ok, ~n Result as expected:~p~n",[AcTuAlReS]),
		AcTuAlReS
	end()).

-define(match(ExpectedRes,Expr),
	fun() ->
		try Expr of
		    _AR_0 = ExpectedRes ->
			?verbose("ok, ~n Result as expected:~p~n",[_AR_0]),
			{success,_AR_0};
		    _AR_0 ->
			?error("Not Matching Actual result was:~n ~p~n",[_AR_0]),
			{fail,_AR_0}
		catch
		    exit:{aborted, _ER_1} when
			  element(1, _ER_1) =:= node_not_running;
			  element(1, _ER_1) =:= bad_commit;
			  element(1, _ER_1) =:= cyclic ->
			%% Need to re-raise these to restart transaction
			erlang:raise(exit, {aborted, _ER_1}, erlang:get_stacktrace());
		    exit:_AR_1 ->
			case fun(_AR_EXIT_) -> {'EXIT', _AR_EXIT_} end(_AR_1) of
			    _AR_2 = ExpectedRes ->
				?verbose("ok, ~n Result as expected:~p~n",[_AR_2]),
				{success,_AR_2};
			    _AR_2 ->
				?error("Not Matching Actual result was:~n ~p~n ~p~n",
				       [_AR_2, erlang:get_stacktrace()]),
				{fail,_AR_2}
			end;
		    _T1_:_AR_1 ->
			?error("Not Matching Actual result was:~n ~p~n  ~p~n",
			       [{_T1_,_AR_1}, erlang:get_stacktrace()]),
			{fail,{_T1_,_AR_1}}
		end
	end()).

-define(match_inverse(NotExpectedRes,Expr),
	fun() ->
		AcTuAlReS = (catch (Expr)),
		case AcTuAlReS of
		    NotExpectedRes ->
			?error("Not matching Actual result was:~n ~p~n",
			       [AcTuAlReS]),
			{fail,AcTuAlReS};
		    _ ->
			?verbose("ok, ~n Result as expected: ~p~n",[AcTuAlReS]),
			{success,AcTuAlReS}
		end
	end()).

-define(match_receive(ExpectedMsg),
	?match(ExpectedMsg,mnesia2_test_lib:pick_msg())).

%% ExpectedMsgs must be completely bound
-define(match_multi_receive(ExpectedMsgs),
	fun() ->
		TmPeXpCtEdMsGs = lists:sort(ExpectedMsgs),
		?match(TmPeXpCtEdMsGs,
		       lists:sort(lists:map(fun(_) ->
						    mnesia2_test_lib:pick_msg()
					    end,
					    TmPeXpCtEdMsGs)))
	end()).

-define(start_activities(Nodes),
	mnesia2_test_lib:start_activities(Nodes)).

-define(start_transactions(Pids),
	mnesia2_test_lib:start_transactions(Pids)).

-define(acquire_nodes(N, Config),
	mnesia2_test_lib:prepare_test_case([{init_test_case, [mnesia2]},
					   delete_schema,
					   create_schema,
					   {start_appls, [mnesia2]}],
					  N, Config, ?FILE, ?LINE)).

-define(activate_debug_fun(I, F, C),
	mnesia2_lib:activate_debug_fun(I, F, C, ?FILE, ?LINE)).

-define(remote_activate_debug_fun(N, I, F, C),
	?match(ok, mnesia2_test_lib:remote_activate_debug_fun(N, I, F, C,
							     ?FILE, ?LINE))).

-define(deactivate_debug_fun(I),
	mnesia2_lib:deactivate_debug_fun(I, ?FILE, ?LINE)).

-define(remote_deactivate_debug_fun(N, I),
	rpc:call(N, mnesia2_lib, deactivate_debug_fun, [I, ?FILE, ?LINE])).

-define(is_debug_compiled,
	case mnesia2_lib:is_debug_compiled() of
	    false ->
		?skip("Mnesia2 is not debug compiled, test case ignored.~n", []);
	    _OhTeR ->
		ok
	end).

-define(needs_disc(Config),
	case mnesia2_test_lib:diskless(Config) of
	    false ->
		ok;
	    true ->
		?skip("Must have disc, test case ignored.~n", [])
	end).

-define(verify_mnesia2(Ups, Downs),
	mnesia2_test_lib:verify_mnesia2(Ups, Downs, ?FILE, ?LINE)).
