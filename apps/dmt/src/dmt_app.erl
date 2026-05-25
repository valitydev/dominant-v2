%%%-------------------------------------------------------------------
%% @doc dmt public API
%% @end
%%%-------------------------------------------------------------------

-module(dmt_app).

-behaviour(application).

-export([start/2, stop/1]).

-spec start(application:start_type(), term()) ->
    {ok, pid()} | {ok, pid(), term()} | {error, term()}.
start(_StartType, _StartArgs) ->
    case dmt_sup:start_link() of
        ignore -> {error, ignore};
        Other -> Other
    end.

-spec stop(term()) -> ok.
stop(_State) ->
    ok.

%% internal functions
