-module(dmt_repository_client_handler).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

-behaviour(woody_server_thrift_handler).

-define(EPGPOOL, default_pool).

-export([handle_function/4]).

-type options() :: dmt_api_woody_utils:handler_options().

-export_type([options/0]).

-spec handle_function(woody:func(), woody:args(), woody_context:ctx(), options()) ->
    {ok, woody:result()} | no_return().
handle_function(Function, Args, WoodyContext0, Options) ->
    DefaultDeadline = woody_deadline:from_timeout(default_handling_timeout(Options)),
    WoodyContext = dmt_api_woody_utils:ensure_woody_deadline_set(WoodyContext0, DefaultDeadline),
    %% Cast: `woody:args()` is `tuple() | any()`; each `do_handle_function/4`
    %% clause pattern-matches a specific arg tuple guaranteed by the thrift
    %% schema. The shape is enforced by woody at deserialisation, not by
    %% the type system, so we cross the trust boundary explicitly here.
    do_handle_function(Function, eqwalizer:dynamic_cast(Args), WoodyContext, Options).

-spec default_handling_timeout(options()) -> timeout().
default_handling_timeout(#{default_handling_timeout := Timeout}) ->
    Timeout.

-spec do_handle_function(woody:func(), eqwalizer:dynamic(tuple()), woody_context:ctx(), options()) ->
    {ok, woody:result()} | no_return().
do_handle_function('CheckoutObject', {VersionRef, ObjectRef}, _Context, _Options) ->
    %% Fetch the object based on VersionReference and Reference
    case dmt_repository:get_object(?EPGPOOL, VersionRef, ObjectRef) of
        {ok, Object} ->
            {ok, Object};
        {error, version_not_found} ->
            woody_error:raise(business, #domain_conf_v2_VersionNotFound{});
        {error, {object_not_found, _Ref}} ->
            woody_error:raise(business, #domain_conf_v2_ObjectNotFound{})
    end;
do_handle_function('CheckoutObjectWithReferences', {VersionRef, ObjectRef}, _Context, _Options) ->
    %% Fetch the object based on VersionReference and Reference
    case dmt_repository:get_object_with_references(?EPGPOOL, VersionRef, ObjectRef) of
        {ok, Object} ->
            {ok, Object};
        {error, version_not_found} ->
            woody_error:raise(business, #domain_conf_v2_VersionNotFound{});
        {error, {object_not_found, _Ref}} ->
            woody_error:raise(business, #domain_conf_v2_ObjectNotFound{})
    end;
do_handle_function('CheckoutObjects', {VersionRef, ObjectRefs}, _Context, _Options) ->
    %% Fetch multiple objects based on VersionReference and Reference list
    case dmt_repository:get_objects(?EPGPOOL, VersionRef, ObjectRefs) of
        {ok, Objects} ->
            {ok, Objects};
        {error, version_not_found} ->
            woody_error:raise(business, #domain_conf_v2_VersionNotFound{})
    end;
do_handle_function('CheckoutSnapshot', {Version}, _Context, _Options) ->
    %% Fetch all objects based on VersionReference
    case dmt_repository:get_snapshot(?EPGPOOL, Version) of
        {ok, Snapshot} ->
            {ok, Snapshot};
        {error, version_not_found} ->
            woody_error:raise(business, #domain_conf_v2_VersionNotFound{})
    end.
