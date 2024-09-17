-module(dmt_v2_repository_handler).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

%% API
-export([handle_function/4]).

handle_function(Function, Args, WoodyContext0, Options) ->
    DefaultDeadline = woody_deadline:from_timeout(default_handling_timeout(Options)),
    WoodyContext = dmt_v2_api_woody_utils:ensure_woody_deadline_set(WoodyContext0, DefaultDeadline),
    do_handle_function(Function, Args, WoodyContext, Options).

do_handle_function('Commit', {Version, Commit, CreatedBy}, _Context, _Options) ->
    case dmt_v2_repository:commit(Version, Commit, CreatedBy) of
        {ok, VersionNext} ->
            {ok, VersionNext};
        {error, {operation_error, Error}} ->
            woody_error:raise(business, handle_operation_error(Error));
        {error, version_not_found} ->
            woody_error:raise(business, #domain_conf_v2_VersionNotFound{});
        {error, head_mismatch} ->
            woody_error:raise(business, #domain_conf_v2_ObsoleteCommitVersion{});
        {error, migration_in_progress} ->
            woody_error:raise(system, {internal, resource_unavailable, <<"Migration in progress. Please, stand by.">>})
    end.

default_handling_timeout(#{default_handling_timeout := Timeout}) ->
    Timeout.

handle_operation_error({conflict, Conflict}) ->
    #domain_conf_v2_OperationConflict{
        conflict = handle_operation_conflict(Conflict)
    };
handle_operation_error({invalid, Invalid}) ->
    #domain_conf_v2_OperationInvalid{
        errors = handle_operation_invalid(Invalid)
    }.

handle_operation_conflict({object_already_exists, Ref}) ->
    {object_already_exists, #domain_conf_v2_ObjectAlreadyExistsConflict{object_ref = Ref}};
handle_operation_conflict({object_not_found, Ref}) ->
    {object_not_found, #domain_conf_v2_ObjectNotFoundConflict{object_ref = Ref}};
handle_operation_conflict({object_reference_mismatch, Ref}) ->
    {object_reference_mismatch, #domain_conf_v2_ObjectReferenceMismatchConflict{object_ref = Ref}}.

handle_operation_invalid({objects_not_exist, Refs}) ->
    [
        {object_not_exists, #domain_conf_v2_NonexistantObject{
            object_ref = Ref,
            referenced_by = ReferencedBy
        }}
     || {Ref, ReferencedBy} <- Refs
    ];
handle_operation_invalid({object_reference_cycles, Cycles}) ->
    [
        {object_reference_cycle, #domain_conf_v2_ObjectReferenceCycle{cycle = Cycle}}
     || Cycle <- Cycles
    ].
