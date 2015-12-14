%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2004-2011. All Rights Reserved.
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
-module(mnesia2_qlc_test).

-compile(export_all).

-export([all/0,groups/0,init_per_group/2,end_per_group/2]).

-include("mnesia2_test_lib.hrl").
-include_lib("stdlib/include/qlc.hrl"). 

init_per_testcase(Func, Conf) ->
    setup(Conf),
    mnesia2_test_lib:init_per_testcase(Func, Conf).

end_per_testcase(Func, Conf) ->
    mnesia2_test_lib:end_per_testcase(Func, Conf).

all() -> 
    case code:which(qlc) of
	non_existing -> [];
	_ -> all_qlc()
    end.

groups() -> 
    [{dirty, [],
      [dirty_nice_ram_copies, dirty_nice_disc_copies,
       dirty_nice_disc_only_copies]},
     {trans, [],
      [trans_nice_ram_copies, trans_nice_disc_copies,
       trans_nice_disc_only_copies, {group, atomic}]},
     {atomic, [], [atomic_eval]}].

init_per_group(_GroupName, Config) ->
    Config.

end_per_group(_GroupName, Config) ->
    Config.


all_qlc() -> 
    [{group, dirty}, {group, trans}, frag, info,
     mnesia2_down].

init_testcases(Type,Config) ->
    Nodes = [N1,N2] = ?acquire_nodes(2, Config),
    ?match({atomic, ok}, mnesia2:create_table(a, [{Type,[N1]}, {index,[3]}])),
    ?match({atomic, ok}, mnesia2:create_table(b, [{Type,[N2]}])),
    Write = fun(Id) -> 
		    ok = mnesia2:write({a, {a,Id}, 100 - Id}),
		    ok = mnesia2:write({b, {b,100-Id}, Id})
	    end,
    All = fun() -> [Write(Id) || Id <- lists:seq(1,10)], ok end,
    ?match({atomic, ok}, mnesia2:sync_transaction(All)),
    ?match({atomic, [{b, {b,100-1}, 1}]}, mnesia2:transaction(fun() -> mnesia2:read({b, {b, 99}}) end)),
    Nodes.

%% Test cases       

dirty_nice_ram_copies(Setup) -> dirty_nice(Setup,ram_copies).
dirty_nice_disc_copies(Setup) -> dirty_nice(Setup,disc_copies).
dirty_nice_disc_only_copies(Setup) -> dirty_nice(Setup,disc_only_copies).

dirty_nice(suite, _) -> [];
dirty_nice(doc, _) -> [];
dirty_nice(Config, Type) when is_list(Config) ->
    Ns = init_testcases(Type,Config),
    QA = handle(<<"[Q || Q = {_,{_,Key},Val} <- mnesia2:table(a),"
		 "     Val == 90 + Key]">>),
    QB = handle(<<"[Q || Q = {_,{_,Key},Val} <- mnesia2:table(b),"
		 "     Key == 90 + Val]">>),
    QC = qlc:sort(mnesia2:table(a, [{n_objects,1}, {lock,write}, {traverse, select}])),
    QD = qlc:sort(mnesia2:table(a, [{n_objects,1}, {traverse,{select,[{'$1',[],['$1']}]}}])),

    FA = fun() -> qlc:e(QA) end,
    FB = fun() -> qlc:e(QB) end,
    FC = fun() -> qlc:e(QC) end,
    FD = fun() -> qlc:e(QD) end,

    %% Currently unsupported
    ?match({'EXIT',{aborted,no_transaction}}, FA()),
    ?match({'EXIT',{aborted,no_transaction}}, FB()),
    %%
    CRes = lists:sort(mnesia2:dirty_match_object(a, {'_','_','_'})),
    ?match([{a,{a,5},95}], mnesia2:async_dirty(FA)),
    ?match([{b,{b,95},5}], mnesia2:async_dirty(FB)),
    ?match(CRes, mnesia2:async_dirty(FC)),
    ?match(CRes, mnesia2:async_dirty(FD)),
    ?match([{a,{a,5},95}], mnesia2:sync_dirty(FA)),
    ?match([{b,{b,95},5}], mnesia2:sync_dirty(FB)),
    ?match(CRes, mnesia2:sync_dirty(FC)),
    ?match([{a,{a,5},95}], mnesia2:activity(async_dirty, FA)),
    ?match([{b,{b,95},5}], mnesia2:activity(async_dirty, FB)),
    ?match([{a,{a,5},95}], mnesia2:activity(sync_dirty, FA)),
    ?match([{b,{b,95},5}], mnesia2:activity(sync_dirty, FB)),
    ?match(CRes, mnesia2:activity(async_dirty,FC)),
    case Type of
	disc_only_copies -> skip;
	_ -> 
	    ?match([{a,{a,5},95}], mnesia2:ets(FA)),
	    ?match([{a,{a,5},95}], mnesia2:activity(ets, FA))
    end,
    ?verify_mnesia2(Ns, []).


trans_nice_ram_copies(Setup) -> trans_nice(Setup,ram_copies).
trans_nice_disc_copies(Setup) -> trans_nice(Setup,disc_copies).
trans_nice_disc_only_copies(Setup) -> trans_nice(Setup,disc_only_copies).

trans_nice(suite, _) -> [];
trans_nice(doc, _) -> [];
trans_nice(Config, Type) when is_list(Config) ->
    Ns = init_testcases(Type,Config),
    QA = handle(<<"[Q || Q = {_,{_,Key},Val} <- mnesia2:table(a),"
		 "     Val == 90 + Key]">>),
    QB = handle(<<"[Q || Q = {_,{_,Key},Val} <- mnesia2:table(b),"
		 "     Key == 90 + Val]">>),
    QC = handle(recs(), 
 		<<"[Q || Q = #a{v=91} <- mnesia2:table(a)]"
 		 >>),

    QD = qlc:sort(mnesia2:table(a, [{n_objects,1}, {lock,write}, {traverse, select}])),
    QE = qlc:sort(mnesia2:table(a, [{n_objects,1}, {traverse,{select,[{'$1',[],['$1']}]}}])),

    DRes = lists:sort(mnesia2:dirty_match_object(a, {'_','_','_'})),

    FA = fun() -> qlc:e(QA) end,
    FB = fun() -> qlc:e(QB) end,
    FC = fun() -> qlc:e(QC) end,
    FD = fun() -> qlc:e(QD) end,
    FE = fun() -> qlc:e(QE) end,
		  
    ?match({atomic,[{a,{a,5},95}]}, mnesia2:transaction(FA)),
    ?match({atomic,[{b,{b,95},5}]}, mnesia2:transaction(FB)),
    ?match({atomic,[{a,{a,9},91}]}, mnesia2:transaction(FC)),
    ?match({atomic,[{a,{a,5},95}]}, mnesia2:sync_transaction(FA)),
    ?match({atomic,[{b,{b,95},5}]}, mnesia2:sync_transaction(FB)),
    ?match({atomic,[{a,{a,9},91}]}, mnesia2:sync_transaction(FC)),
    ?match([{a,{a,5},95}], mnesia2:activity(transaction,FA)),
    ?match([{b,{b,95},5}], mnesia2:activity(transaction,FB)),
    ?match([{a,{a,9},91}], mnesia2:activity(transaction,FC)),
    ?match([{a,{a,5},95}], mnesia2:activity(sync_transaction,FA)),
    ?match([{b,{b,95},5}], mnesia2:activity(sync_transaction,FB)),
    ?match([{a,{a,9},91}], mnesia2:activity(sync_transaction,FC)),

    ?match({atomic, DRes}, mnesia2:transaction(FD)),
    ?match({atomic, DRes}, mnesia2:transaction(FE)),

    Rest = fun(Cursor,Loop) -> 
		   case qlc:next_answers(Cursor, 1) of
		       [] -> [];
		       [A]-> [A|Loop(Cursor,Loop)] 
		   end
	   end,
    Loop = fun() -> 
		   Cursor = qlc:cursor(QD),
		   Rest(Cursor,Rest)
	   end,
    ?match({atomic, DRes}, mnesia2:transaction(Loop)),

    ?verify_mnesia2(Ns, []).

%% -record(a, {k,v}).
%% -record(b, {k,v}).
%% -record(k, {t,v}).

recs() ->
    <<"-record(a, {k,v}). "
      "-record(b, {k,v}). "
      "-record(k, {t,v}). "
     >>.
 

atomic_eval(suite) -> [];
atomic_eval(doc) -> []; 
atomic_eval(Config) ->
    Ns = init_testcases(ram_copies, Config),    
    Q1 = handle(recs(), 
		<<"[Q || Q = #a{k={_,9}} <- mnesia2:table(a)]"
		 >>),
    Eval = fun(Q) -> 
		   {qlc:e(Q),
		    mnesia2:system_info(held_locks)}
	   end,
    Self = self(),
    ?match({[{a,{a,9},91}], [{{a,'______WHOLETABLE_____'},read,{tid,_,Self}}]},
	   ok(Eval,[Q1])),
    
    Q2 = handle(recs(), 
		<<"[Q || Q = #a{k={a,9}} <- mnesia2:table(a)]"
		 >>),
    
    ?match({[{a,{a,9},91}],[{{a,{a,9}},read,{tid,_,Self}}]},
	   ok(Eval,[Q2])),

    Flush = fun(Loop) -> %% Clean queue
		    receive _ -> Loop(Loop) 
		    after 0 -> ok end
	    end,
    
    Flush(Flush),

    GrabLock = fun(Father) ->  
		       mnesia2:read(a, {a,9}, write),
		       Father ! locked,
		       receive cont -> ok end end,

    Pid1 = spawn(fun() -> ?match(ok, ok(GrabLock, [Self])) end),    
    ?match(locked,receive locked -> locked after 5000 -> timeout end), %% Wait

    put(count,0),
    Restart = fun(Locker,Fun) ->
		      Count = get(count),
		      case {Count,(catch Fun())}  of
			  {0, {'EXIT', R}} ->
			      Locker ! cont,
			      put(count, Count+1),
			      erlang:yield(),
			      exit(R);
			  Else ->
			      Else
		      end
	      end,
    
    ?match({1,{[{a,{a,9},91}], [{{a,'______WHOLETABLE_____'},read,{tid,_,Self}}]}},
	   ok(Restart,[Pid1,fun() -> Eval(Q1) end])),
    
    Pid2 = spawn(fun() -> ?match(ok, ok(GrabLock, [Self])) end),
    ?match(locked,receive locked -> locked after 5000 -> timeout end), %% Wait
    put(count,0),
    ?match({1,{[{a,{a,9},91}],[{{a,{a,9}},read,{tid,_,Self}}]}},
	   ok(Restart,[Pid2, fun() -> Eval(Q2) end])),

%% Basic test     
    Cursor = fun() ->
		     QC = qlc:cursor(Q1),
		     qlc:next_answers(QC) 
	     end,

    ?match([{a,{a,9},91}], ok(Cursor, [])),
    %% Lock 

    Pid3 = spawn(fun() -> ?match(ok, ok(GrabLock, [Self])) end),    
    ?match(locked,receive locked -> locked after 5000 -> timeout end), %% Wait
    put(count,0),
    
    ?match({1,[{a,{a,9},91}]}, ok(Restart,[Pid3, Cursor])),
    QC1 = ok(fun() -> qlc:cursor(Q1) end, []),
    ?match({'EXIT', _},  (catch qlc:next_answers(QC1))),
    ?match({aborted,_},  ok(fun()->qlc:next_answers(QC1)end,[])),
    ?verify_mnesia2(Ns, []).


frag(suite) -> [];
frag(doc) -> [];
frag(Config) ->
    Ns = init_testcases(ram_copies,Config),
    QA = handle(<<"[Q || Q = {_,{_,Key},Val} <- mnesia2:table(a),"
		 "     Val == 90 + Key]">>),
    QB = handle(<<"[Q || Q = {_,{_,Key},Val} <- mnesia2:table(b),"
		 "     Key == 90 + Val]">>),
    
    Activate = 
	fun(Tab) ->
		?match({atomic,ok},mnesia2:change_table_frag(Tab, {activate, []})),
		Dist = mnesia2_frag_test:frag_dist(Tab),
		?match({atomic,ok},mnesia2:change_table_frag(Tab,{add_frag,Dist}))
	end,
    Activate(a),
    Activate(b),

    Fun = fun(Tab) -> mnesia2:table_info(Tab, frag_names) end,
    FTs = mnesia2:activity(sync_dirty, Fun, [a], mnesia2_frag) ++
	mnesia2:activity(sync_dirty, Fun, [b], mnesia2_frag),
    Size = fun(Tab) -> mnesia2:dirty_rpc(Tab, mnesia2, table_info, [Tab,size]) end,

    %% Verify that all data doesn't belong to the same frag.
    ?match([], [{Tab,Size(Tab)} || Tab <- FTs,
				   Size(Tab) =< 0]),
    
    FA = fun() -> qlc:e(QA) end,
    FB = fun() -> qlc:e(QB) end,
    ?match([{a,{a,5},95}], mnesia2:activity(transaction,FA,[],mnesia2_frag)),
    ?match([{b,{b,95},5}], mnesia2:activity(transaction,FB,[],mnesia2_frag)),
    
    ?verify_mnesia2(Ns, []).

info(suite) -> [];
info(doc) -> [];
info(Config) ->
    Ns = init_testcases(ram_copies, Config),
    Q1 = handle(recs(), 
		<<"[Q || Q = #a{k={_,9}} <- mnesia2:table(a)]"
		 >>),
    
    Q2 = handle(recs(), 
		<<"[Q || Q = #a{k={a,9}} <- mnesia2:table(a)]"
		 >>),
    
    Q3 = handle(recs(), 
		<<"[Q || Q = #a{v=91} <- mnesia2:table(a)]"
		 >>),
    
    %% FIXME compile and check results!
    
    ?match(ok,io:format("~s~n",[qlc:info(Q1)])),
    ?match(ok,io:format("~s~n",[qlc:info(Q2)])),
    ?match(ok,io:format("~s~n",[qlc:info(Q3)])),
 
    ?verify_mnesia2(Ns, []).

ok(Fun,A) ->
    case mnesia2:transaction(Fun,A) of
	{atomic, R} -> R;
	E -> E
    end.


mnesia2_down(suite) -> [];
mnesia2_down(doc) ->
    ["Test bug OTP-7968, which crashed mnesia2 when a"
     "mnesia2_down came after qlc had been invoked"];
mnesia2_down(Config) when is_list(Config) ->
    [N1,N2] = init_testcases(ram_copies,Config),
    QB = handle(<<"[Q || Q = {_,{_,Key},Val} <- mnesia2:table(b),"
		 "     Val == Key - 90]">>),

    Tester = self(),    
    
    Eval = fun() -> 
		   Cursor = qlc:cursor(QB), %% Forces another process
		   Res = qlc:next_answers(Cursor),
		   Tester ! {qlc, self(), Res},
		   {Mod, Tid, Ts} = get(mnesia2_activity_state),
		   receive
		       continue ->
			   io:format("Continuing ~p ~p ~n",[self(), {Mod, Tid, Ts}]),
			   io:format("ETS ~p~n",[ets:tab2list(element(2,Ts))]),
			   io:format("~p~n",[process_info(self(),messages)]),
			   Res
		   end
	   end,
    spawn(fun() -> TransRes = mnesia2:transaction(Eval), Tester ! {test,TransRes} end),

    TMInfo = fun() ->
		     TmInfo = mnesia2_tm:get_info(5000),
		     mnesia2_tm:display_info(user, TmInfo)
	     end,
    receive
	{qlc, QPid, QRes} ->
	    ?match([{b,{b,95},5}], QRes),
	    TMInfo(),
	    mnesia2_test_lib:kill_mnesia2([N2]),
	    %%timer:sleep(1000),
	    QPid ! continue
    after 2000 ->
	    exit(timeout1)
    end,

    receive
	{test, QRes2} ->
	    ?match({atomic, [{b,{b,95},5}]}, QRes2)
    after 2000 ->
	    exit(timeout2)
    end,
    
    ?verify_mnesia2([N1], [N2]).


nested_qlc(suite) -> [];
nested_qlc(doc) ->
    ["Test bug in OTP-7968 (the second problem) where nested"
     "transaction don't work as expected"];
nested_qlc(Config) when is_list(Config) ->
    Ns = init_testcases(ram_copies,Config),    
    Res = as_with_bs(),
    ?match([_|_], Res),
    top_as_with_some_bs(10),
    
    ?verify_mnesia2(Ns, []).


%% Code from Daniel 
bs_by_a_id(A_id) ->
    find(qlc:q([ B || B={_,_,F_id} <- mnesia2:table(b), F_id == A_id])).

as_with_bs() ->
    find(qlc:q([ {A,bs_by_a_id(Id)} ||
		   A = {_, {a,Id}, _} <- mnesia2:table(a)])).

top_as_with_some_bs(Limit) ->
    top(
      qlc:q([ {A,bs_by_a_id(Id)} ||
		A = {_, {a,Id}, _} <- mnesia2:table(a)]),
      Limit,
      fun(A1,A2) -> A1 < A2  end
     ).

% --- utils

find(Q) ->
    F = fun() -> qlc:e(Q) end,
    {atomic, Res} = mnesia2:transaction(F),
    Res.

% --- it returns top Limit results from query Q ordered by Order sort function
top(Q, Limit, Order) ->
    Do = fun() ->
		 OQ = qlc:sort(Q, [{order,Order}]),
		 QC = qlc:cursor(OQ),
		 Res = qlc:next_answers(QC, Limit),
		 qlc:delete_cursor(QC),
		 Res
	 end,
    {atomic, Res} = mnesia2:transaction(Do),
    Res.

%% To keep mnesia2 suite backward compatible,
%% we compile the queries in runtime when qlc is available
%% Compiles and returns a handle to a qlc
handle(Expr) ->
    handle(<<>>,Expr).
handle(Records,Expr) ->
    case catch handle2(Records,Expr) of
	{ok, Handle} ->
	    Handle;
	Else ->
	    ?match(ok, Else)
    end.

handle2(Records,Expr) ->
    {FN,Mod} = temp_name(),
    ModStr = list_to_binary("-module(" ++ atom_to_list(Mod) ++ ").\n"),
    Prog = <<
	    ModStr/binary,
	    "-include_lib(\"stdlib/include/qlc.hrl\").\n",
	    "-export([tmp/0]).\n",
	    Records/binary,"\n",
	    "tmp() ->\n",
%%	    "   _ = (catch throw(fvalue_not_reset)),"
	    "   qlc:q( ",
	    Expr/binary,").\n">>,

    ?match(ok,file:write_file(FN,Prog)),
    {ok,Forms} = epp:parse_file(FN,"",""),
    {ok,Mod,Bin} = compile:forms(Forms),
    code:load_binary(Mod,FN,Bin),
    {ok, Mod:tmp()}.

setup(Config) ->
    put(mts_config,Config),
    put(mts_tf_counter,0).

temp_name() ->
    Conf = get(mts_config),
    C = get(mts_tf_counter),
    put(mts_tf_counter,C+1),
    {filename:join([proplists:get_value(priv_dir,Conf, "."),
		    "tempfile"++integer_to_list(C)++".tmp"]),
     list_to_atom("tmp" ++ integer_to_list(C))}.
