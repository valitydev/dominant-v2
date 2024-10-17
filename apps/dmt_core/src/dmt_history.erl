-module(dmt_history).

-export([head/1]).
-export([head/2]).
-export([travel/3]).

-include_lib("damsel/include/dmsl_domain_conf_thrift.hrl").

-type history() :: dmsl_domain_conf_thrift:'History'().
-type version() :: dmsl_domain_conf_thrift:'Version'().
-type snapshot() :: dmsl_domain_conf_thrift:'Snapshot'().

-spec head(history()) -> {ok, snapshot()} | {error, dmt_domain:operation_error()}.
head(History) ->
    head(History, #domain_conf_Snapshot{version = 0, domain = dmt_domain:new()}).

-spec head(history(), snapshot()) -> {ok, snapshot()} | {error, dmt_domain:operation_error()}.
head(History, Snapshot) when map_size(History) =:= 0 ->
    {ok, Snapshot};
head(History, Snapshot) ->
    Head = lists:max(maps:keys(History)),
    travel(Head, History, Snapshot).

-spec travel(version(), history(), snapshot()) -> {ok, snapshot()} | {error, dmt_domain:operation_error()}.
travel(To, _History, #domain_conf_Snapshot{version = From} = Snapshot) when To =:= From ->
    {ok, Snapshot};
travel(To, History, #domain_conf_Snapshot{version = From, domain = Domain}) when To > From ->
    #domain_conf_Commit{ops = Ops} = maps:get(From + 1, History),
    case dmt_domain:apply_operations(Ops, Domain) of
        {ok, NewDomain} ->
            NextSnapshot = #domain_conf_Snapshot{
                version = From + 1,
                domain = NewDomain
            },
            travel(To, History, NextSnapshot);
        {error, _} = Error ->
            Error
    end;
travel(To, History, #domain_conf_Snapshot{version = From, domain = Domain}) when To < From ->
    #domain_conf_Commit{ops = Ops} = maps:get(From, History),
    case dmt_domain:revert_operations(Ops, Domain) of
        {ok, NewDomain} ->
            PreviousSnapshot = #domain_conf_Snapshot{
                version = From - 1,
                domain = NewDomain
            },
            travel(To, History, PreviousSnapshot);
        {error, _} = Error ->
            Error
    end.
