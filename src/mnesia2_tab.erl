%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2016. All Rights Reserved.
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
-module(mnesia2_tab).

-export([init/0,
         stop/0,
         new/1,
         is_blocked/1,
         block/1, unblock/1]).

-define(BLOCKED_TABS_TABLE, ?MODULE).

-spec init() -> ok.
init() ->
    ?BLOCKED_TABS_TABLE =
        ets:new(?BLOCKED_TABS_TABLE, [set, named_table,
                                      public,
                                      {write_concurrency, true},
                                      {read_concurrency, true}]),
    ok.

-spec stop() -> ok.
stop() ->
    true = ets:delete(?BLOCKED_TABS_TABLE),
    ok.

-spec new(Tab :: atom()) -> ok.
new(Tab) ->
    true = ets:insert_new(?BLOCKED_TABS_TABLE, {Tab, 0}),
    ok.

-spec is_blocked(Tab :: atom()) -> boolean().
is_blocked(Tab) ->
    case ets:lookup(?BLOCKED_TABS_TABLE, Tab) of
        [] -> false;
        [{Tab, 0}] -> false;
        [{Tab, N}] when N > 0 -> true
    end.

-spec block(Tab :: atom()) -> integer().
block(Tab) ->
    %% make sure the table exists
    ok = case ets:lookup(?BLOCKED_TABS_TABLE, Tab) of
            [] -> new(Tab);
            _ -> ok
         end,
    %% bump the counter (which is the second element in the tuple)
    %% by 1
    ets:update_counter(?BLOCKED_TABS_TABLE,
                       Tab, {2, 1}).

-spec unblock(Tab :: atom()) -> integer().
unblock(Tab) ->
    %% decrease the counter (which is the second element in the tuple)
    %% by 1, make sure it doesn't fall below 0
    ets:update_counter(?BLOCKED_TABS_TABLE,
                       Tab, {2, -1, 0, 0}).
