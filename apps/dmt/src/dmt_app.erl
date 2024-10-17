%%%-------------------------------------------------------------------
%% @doc dmt public API
%% @end
%%%-------------------------------------------------------------------

-module(dmt_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    dmt_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
