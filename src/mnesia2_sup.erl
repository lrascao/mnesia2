%%
%% %CopyrightBegin%
%% 
%% Copyright Ericsson AB 1996-2013. All Rights Reserved.
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
%% Supervisor for the entire Mnesia2 application

-module(mnesia2_sup).

-behaviour(application).
-behaviour(supervisor).

-export([start/0, start/2, init/1, stop/1, start_event/0, kill/0]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% application and suprvisor callback functions

start(normal, Args) ->
    SupName = {local,?MODULE},
    case supervisor:start_link(SupName, ?MODULE, [Args]) of
	{ok, Pid} ->
	    {ok, Pid, {normal, Args}};
	Error -> 
	    Error
    end;
start(_, _) ->
    {error, badarg}.

start() ->
    SupName = {local,?MODULE},
    supervisor:start_link(SupName, ?MODULE, []).

stop(_StartArgs) ->
    ok.

init([]) -> % Supervisor
    init();
init([[]]) -> % Application
    init();
init(BadArg) ->
    {error, {badarg, BadArg}}.
    
init() ->
    ok = mnesia2_tab:init(),
    Flags = {one_for_all, 0, 3600}, % Should be rest_for_one policy

    Event = event_procs(),
    Kernel = kernel_procs(),

    {ok, {Flags, Event ++ Kernel}}.

event_procs() ->
    KillAfter = timer:seconds(30),
    KA = mnesia2_kernel_sup:supervisor_timeout(KillAfter),
    E = mnesia2_event,
    [{E, {?MODULE, start_event, []}, permanent, KA, worker, [E, gen_event]}].

kernel_procs() ->
    K = mnesia2_kernel_sup,
    KA = infinity,
    [{K, {K, start, []}, permanent, KA, supervisor, [K, supervisor]}].

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% event handler

start_event() ->
    case gen_event:start_link({local, mnesia2_event}) of
	{ok, Pid} ->
	    case add_event_handler() of
		ok -> 
		    {ok, Pid};
		Error ->
		    Error
	    end;
	Error  ->
	    Error
    end.

add_event_handler() ->
    Handler = mnesia2_monitor:get_env(event_module),
    gen_event:add_handler(mnesia2_event, Handler, []).
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% debug functions

kill() ->
    Mnesia2 = [mnesia2_fallback | mnesia2:ms()],
    Kill = fun(Name) -> catch exit(whereis(Name), kill) end,
    lists:foreach(Kill, Mnesia2),
    lists:foreach(fun ensure_dead/1, Mnesia2),
    timer:sleep(10),
    case lists:keymember(mnesia2, 1, application:which_applications()) of
	true -> kill();
	false -> ok
    end.

ensure_dead(Name) ->
    case whereis(Name) of
	undefined ->
	    ok;
	Pid when is_pid(Pid) ->
	    exit(Pid, kill),
	    timer:sleep(10),
	    ensure_dead(Name)
    end.

