%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 2014-2015. All Rights Reserved.
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
%% If your code need to be able to execute on ERTS versions both
%% earlier and later than 7.0, the best approach is to use the new
%% time API introduced in ERTS 7.0 and implement a fallback
%% solution using the old primitives to be used on old ERTS
%% versions. This way your code can automatically take advantage
%% of the improvements in the API when available. This is an
%% example of how to implement such an API, but it can be used
%% as is if you want to. Just add (a preferrably renamed version of)
%% this module to your project, and call the API via this module
%% instead of calling the BIFs directly.
%%

-module(mnesia2_time).

-export([monotonic_time/0,
         time_offset/0,
         timestamp/0,
         unique_integer/0,
         unique_integer/1,
         system_time/0]).

-ifdef(post_otp_18).
monotonic_time() ->
    erlang:monotonic_time().
-endif.
-ifdef(pre_otp_18).
monotonic_time() ->
    %% Use Erlang system time as monotonic time
    erlang_system_time_fallback().
-endif.

-ifdef(post_otp_18).
time_offset() ->
    erlang:time_offset().
-endif.
-ifdef(pre_otp_18).
time_offset() ->
    %% Erlang system time and Erlang monotonic
    %% time are always aligned
    0.
-endif.

-ifdef(post_otp_18).
timestamp() ->
    erlang:timestamp().
-endif.
-ifdef(pre_otp_18).
timestamp() ->
    os:timestamp().
-endif.

unique_integer() ->
    unique_integer([]).

-ifdef(post_otp_18).
unique_integer(Modifiers) ->
    erlang:unique_integer(Modifiers).
-endif.
-ifdef(pre_otp_18).
unique_integer(_Modifiers) ->
    %% now() converted to an integer
    %% fullfill the requirements of
    %% all modifiers: unique, positive,
    %% and monotonic...
    erlang_system_time_fallback().
-endif.

-ifdef(post_otp_18).
system_time() ->
    erlang:system_time().
-endif.
-ifdef(pre_otp_18).
system_time() ->
    erlang_system_time_fallback().
-endif.

%%
%% Internal functions
%%

-ifdef(pre_otp_18).
erlang_system_time_fallback() ->
    {MS, S, US} = os:timestamp(),
    (MS*1000000+S)*1000000+US.
-endif.
