-module(dmt_repository_client_handler).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

-export([handle_function/4]).

handle_function(Function, Args, WoodyContext0, Options) ->
    DefaultDeadline = woody_deadline:from_timeout(default_handling_timeout(Options)),
    WoodyContext = dmt_api_woody_utils:ensure_woody_deadline_set(WoodyContext0, DefaultDeadline),
    do_handle_function(Function, Args, WoodyContext, Options).

default_handling_timeout(#{default_handling_timeout := Timeout}) ->
    Timeout.

do_handle_function('CheckoutObject', {VersionRef, ObjectRef}, _Context, _Options) ->
    %% Fetch the object based on VersionReference and Reference
    case dmt_repository:get_object(VersionRef, ObjectRef) of
        {ok, Object} ->
            {ok, Object};
        {error, global_version_not_found} ->
            woody_error:raise(business, #domain_conf_v2_GlobalVersionNotFound{});
        {error, {object_not_found, _Ref}} ->
            woody_error:raise(business, #domain_conf_v2_ObjectNotFound{});
        {error, Reason} ->
            woody_error:raise(system, {internal, Reason})
    end;
do_handle_function('GetLatestGlobalVersion', _, _Context, _Options) ->
    %% Fetch the object based on VersionReference and Reference
    case dmt_repository:get_latest_global_version() of
        {ok, Version} ->
            {ok, Version};
        {error, Reason} ->
            woody_error:raise(system, {internal, Reason})
    end.
% TODO
% do_handle_function('GetLocalVersions', {Request}, _Context, _Options) ->
%     #domain_conf_v2_GetLocalVersionsRequest{
%         ref = Ref,
%         limit = Limit,
%         continuation_token = ContinuationToken
%     } = Request,
%     %% Retrieve local versions with pagination
%     case dmt_repository:get_local_versions(Ref, Limit, ContinuationToken) of
%         {ok, Versions, NewToken} ->
%             {ok, #domain_conf_v2_GetVersionsResponse{
%                 result = Versions,
%                 continuation_token = NewToken
%             }};
%         {error, object_not_found} ->
%             woody_error:raise(business, #domain_conf_v2_ObjectNotFound{});
%         {error, Reason} ->
%             woody_error:raise(system, {internal, Reason})
%     end;
% TODO
% do_handle_function('GetGlobalVersions', {Request}, _Context, _Options) ->
%     #domain_conf_v2_GetGlobalVersionsRequest{
%         limit = Limit,
%         continuation_token = ContinuationToken
%     } = Request,
%     %% Retrieve global versions with pagination
%     case dmt_repository:get_global_versions(Limit, ContinuationToken) of
%         {ok, Versions, NewToken} ->
%             {ok, #domain_conf_v2_GetVersionsResponse{
%                 result = Versions,
%                 continuation_token = NewToken
%             }};
%         {error, Reason} ->
%             woody_error:raise(system, {internal, Reason})
%     end.
