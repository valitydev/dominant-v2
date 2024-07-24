%%%-------------------------------------------------------------------
%% @doc dominant-v2 public API
%% @end
%%%-------------------------------------------------------------------

-module(dominant-v2_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    dominant-v2_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
