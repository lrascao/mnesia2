%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2010. All Rights Reserved.
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

-module(mnesia2_dbn_meters).
-export([
	 start/0,
	 local_start/0,
	 distr_start/1,
	 start/3
	]).

-record(simple,{key,val=0}).
-define(key,1).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Configuration and start

start() ->
    local_start(),
    distr_start(nodes()).

local_start() ->
    start(one_ram_only,[node()],some_meters()),
    start(one_disc_only,[node()],some_meters()).
    
distr_start([]) ->
    local_only;
distr_start(OtherNodes) when is_list(OtherNodes) ->
    start(ram_and_ram,[node()|OtherNodes],some_meters()),
    start(disc_and_disc,[node()|OtherNodes],some_meters()).

start(Config,Nodes,Meters) ->
    Attrs = record_info(fields,simple),
    Schema = [{name,simple},{type,set},{attributes,Attrs}] ++ config(Config,Nodes),
    L = '====================',
    io:format("~n~p dbn_meters: ~p ~p~nSchema = ~p.~n~n",[L,Config,L,Schema]),
    ok = mnesia2:delete_schema(Nodes),
    ok = mnesia2:create_schema(Nodes),
    rpc:multicall(Nodes, mnesia2, start, []),
    {atomic,_} = mnesia2:create_table(Schema),
    lists:foreach(fun report_meter/1,Meters),
    {atomic, ok} = mnesia2:delete_table(simple),
    rpc:multicall(Nodes, mnesia2, stop, []),
    ok.

config(one_ram_only,[Single|_]) ->
    [{ram_copies,[Single]}];
config(ram_and_ram,[Master|[Slave|_]]) ->
    [{ram_copies,[Master,Slave]}];
config(one_disc_only,[Single|_]) ->
    [{disc_copies,[Single]}];
config(disc_and_disc,[Master|[Slave|_]]) ->
    [{disc_copies,[Master,Slave]}];
config(Config,Nodes) ->
    io:format("<ERROR> Config ~p not supported or too few nodes ~p given~n",[Config,Nodes]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% The various DBN meters
some_meters() ->
    [create,
     open_safe_read,
     open_dirty_read,
     get_int,
     open_update,
     put_int,
     put_int_and_copy,
     dirty_put_int_and_copy,
     start_trans,
     commit_one_update,
     delete,
     dirty_delete
    ].

report_meter(Meter) ->
    Times = 100,
    Micros = repeat_meter(Meter,{atomic,{0,ignore}},Times) div Times,
    io:format("\t~-30w ~-10w nano seconds (mean of ~p repetitions)~n",[Meter,Micros,Times]).

repeat_meter(_Meter,{atomic,{Micros,_Result}},0) ->
    Micros;
repeat_meter(Meter,{atomic,{Micros,_Result}},Times) when Times > 0 ->
    repeat_meter(Meter,catch meter(Meter),Times-1) + Micros;
repeat_meter(Meter,{aborted,Reason},Times) when Times > 0 ->
    io:format("<ERROR>\t~-20w\t,aborted, because ~p~n",[Meter,Reason]),
    0;
repeat_meter(Meter,{'EXIT',Reason},Times) when Times > 0 ->
    io:format("<ERROR>\t~-20w\tcrashed, because ~p~n",[Meter,Reason]),
    0.

meter(create) ->
    Key = 1,
    mnesia2:transaction(fun() -> mnesia2:delete({simple,Key}) end),
    Fun = fun() ->
		  BeforeT = erlang:test(),
		  R = mnesia2:write(#simple{key=Key}),
		  AfterT = mnesia2_time:monotonic_time(),
		  elapsed_time(BeforeT,AfterT,R)
	  end,
    mnesia2:transaction(Fun);

meter(open_safe_read) ->
    Key = 2,
    mnesia2:transaction(fun() -> mnesia2:write(#simple{key=Key}) end),
    Fun = fun() ->
		  BeforeT = mnesia2_time:monotonic_time(),
		  R = mnesia2:read({simple,Key}),
		  AfterT = mnesia2_time:monotonic_time(),
		  elapsed_time(BeforeT,AfterT,R)
	  end,
    mnesia2:transaction(Fun);

meter(open_dirty_read) ->
    Key = 21,
    mnesia2:transaction(fun() -> mnesia2:write(#simple{key=Key}) end),
    Fun = fun() ->
		  BeforeT = mnesia2_time:monotonic_time(),
		  R = mnesia2:dirty_read({simple,Key}),
		  AfterT = mnesia2_time:monotonic_time(),
		  elapsed_time(BeforeT,AfterT,R)
	  end,
    mnesia2:transaction(Fun);

meter(get_int) ->
    Key = 3,
    mnesia2:transaction(fun() -> mnesia2:write(#simple{key=Key}) end),
    Fun = fun() ->
		  [Simple] = mnesia2:read({simple,Key}),
		  BeforeT = mnesia2_time:monotonic_time(),
		  Int = Simple#simple.val,
		  AfterT = mnesia2_time:monotonic_time(),
		  elapsed_time(BeforeT,AfterT,Int)
	  end,
    mnesia2:transaction(Fun);

meter(open_update) ->
    Key = 3,
    mnesia2:transaction(fun() -> mnesia2:write(#simple{key=Key}) end),
    Fun = fun() ->
		  BeforeT = mnesia2_time:monotonic_time(),
		  R = mnesia2:wread({simple,Key}),
		  AfterT = mnesia2_time:monotonic_time(),
		  elapsed_time(BeforeT,AfterT,R)
	  end,
    mnesia2:transaction(Fun);

meter(put_int) ->
    Key = 4,
    mnesia2:transaction(fun() -> mnesia2:write(#simple{key=Key}) end),
    Fun = fun() ->
		  [Simple] = mnesia2:wread({simple,Key}),
		  BeforeT = mnesia2_time:monotonic_time(),
		  R = Simple#simple{val=7},
		  AfterT = mnesia2_time:monotonic_time(),
		  elapsed_time(BeforeT,AfterT,R)
	  end,
    mnesia2:transaction(Fun);

meter(put_int_and_copy) ->
    Key = 5,
    mnesia2:transaction(fun() -> mnesia2:write(#simple{key=Key}) end),
    Fun = fun() ->
		  [Simple] = mnesia2:wread({simple,Key}),
		  BeforeT = mnesia2_time:monotonic_time(),
		  Simple2 = Simple#simple{val=17},
		  R = mnesia2:write(Simple2),
		  AfterT = mnesia2_time:monotonic_time(),
		  elapsed_time(BeforeT,AfterT,R)
	  end,
    mnesia2:transaction(Fun);

meter(dirty_put_int_and_copy) ->
    Key = 55,
    mnesia2:dirty_write(#simple{key=Key}),
    [Simple] = mnesia2:dirty_read({simple,Key}),
    BeforeT = mnesia2_time:monotonic_time(),
    Simple2 = Simple#simple{val=17},
    R = mnesia2:dirty_write(Simple2),
    AfterT = mnesia2_time:monotonic_time(),
    {atomic,elapsed_time(BeforeT,AfterT,R)};

meter(start_trans) ->
    BeforeT = mnesia2_time:monotonic_time(),
    {atomic,AfterT} = mnesia2:transaction(fun() -> mnesia2_time:monotonic_time() end),
    {atomic,elapsed_time(BeforeT,AfterT,ok)};

meter(commit_one_update) ->
    Key = 6,
    mnesia2:transaction(fun() -> mnesia2:write(#simple{key=Key}) end),
    Fun = fun() ->
		  [Simple] = mnesia2:wread({simple,Key}),
		  Simple2 = Simple#simple{val=27},
		  _R = mnesia2:write(Simple2),
		  mnesia2_time:monotonic_time()
	  end,
    {atomic,BeforeT} = mnesia2:transaction(Fun),
    AfterT = mnesia2_time:monotonic_time(),
    {atomic,elapsed_time(BeforeT,AfterT,ok)};

meter(delete) ->
    Key = 7,
    mnesia2:transaction(fun() -> mnesia2:write(#simple{key=Key}) end),
    Fun = fun() ->
		  BeforeT = mnesia2_time:monotonic_time(),
		  R = mnesia2:delete({simple,Key}),
		  AfterT = mnesia2_time:monotonic_time(),
		  elapsed_time(BeforeT,AfterT,R)
	  end,
    mnesia2:transaction(Fun);

meter(dirty_delete) ->
    Key = 75,
    mnesia2:dirty_write(#simple{key=Key}),
    BeforeT = mnesia2_time:monotonic_time(),
    R = mnesia2:dirty_delete({simple,Key}),
    AfterT = mnesia2_time:monotonic_time(),
    {atomic, elapsed_time(BeforeT,AfterT,R)}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Calculate the elapsed time
elapsed_time(BeforeT,AfterT,Result) ->
    {erlang:convert_time_unit(AfterT-BeforeT, native, nano_seconds),Result}.
