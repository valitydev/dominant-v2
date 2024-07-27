%%%-------------------------------------------------------------------
%% @doc dominant_v2 public API
%% @end
%%%-------------------------------------------------------------------

-module(dominant_v2_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    dominant_v2_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
