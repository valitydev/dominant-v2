-module(dmt_repository_client_handler).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

-define(EPGPOOL, default_pool).

-export([handle_function/4]).

handle_function(Function, Args, WoodyContext0, Options) ->
    DefaultDeadline = woody_deadline:from_timeout(default_handling_timeout(Options)),
    WoodyContext = dmt_api_woody_utils:ensure_woody_deadline_set(WoodyContext0, DefaultDeadline),
    do_handle_function(Function, Args, WoodyContext, Options).

default_handling_timeout(#{default_handling_timeout := Timeout}) ->
    Timeout.

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
