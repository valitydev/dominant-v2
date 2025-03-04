-module(dmt_repository).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

%% API

-export([commit/3]).
-export([get_object/3]).
-export([get_latest_version/0]).
-export([get_all_objects_history/1]).
-export([search_objects/1]).

%%

get_object(Worker, {version, V}, ObjectRef) ->
    case get_target_object(Worker, ObjectRef, V) of
        {ok, #{
            version := Version,
            data := Data,
            created_at := CreatedAt,
            created_by := CreatedBy
        }} ->
            % io:format("get_object Data ~p~n", [Data]),
            {ok, #domain_conf_v2_VersionedObject{
                info = #domain_conf_v2_VersionedObjectInfo{
                    version = Version,
                    ref = ObjectRef,
                    changed_at = CreatedAt,
                    changed_by = CreatedBy
                },
                object = Data
            }};
        {error, Reason} ->
            {error, Reason}
    end;
get_object(Worker, {head, #domain_conf_v2_Head{}}, ObjectRef) ->
    case get_latest_target_object(Worker, ObjectRef) of
        {ok, #{
            version := Version,
            data := Data,
            created_at := CreatedAt,
            created_by := CreatedBy
        }} ->
            {ok, #domain_conf_v2_VersionedObject{
                info = #domain_conf_v2_VersionedObjectInfo{
                    version = Version,
                    ref = ObjectRef,
                    changed_at = CreatedAt,
                    changed_by = CreatedBy
                },
                object = Data
            }};
        {error, Reason} ->
            {error, Reason}
    end.

% Done this way to keep hierarchy of calls
get_latest_version() ->
    dmt_database:get_latest_version(default_pool).

get_all_objects_history(_Request) ->
    throw(not_impl).

search_objects(_Request) ->
    throw(not_impl).

assemble_operations(Worker, Operations) ->
    lists:foldl(
        fun assemble_operations_/2,
        {Worker, [], #{}, []},
        Operations
    ).

assemble_operations_(
    Operation,
    {Worker, InsertsAcc, UpdatesAcc, UpdatedObjectsAcc}
) ->
    case Operation of
        {insert, #domain_conf_v2_InsertOp{} = InsertOp} ->
            {ok, NewObject} = dmt_object:new_object(InsertOp),
            #{
                tmp_id := TmpID,
                references := Refers
            } = NewObject,

            Updates1 = update_objects_added_refs(Worker, {temporary, TmpID}, Refers, UpdatesAcc),
            {Worker, [NewObject | InsertsAcc], Updates1, UpdatedObjectsAcc};
        {update, #domain_conf_v2_UpdateOp{object = Object}} ->
            {ok, Ref} = get_object_ref(Object),
            {ok, Changes} = get_original_object_changes(Worker, UpdatesAcc, Ref),
            {ok, ObjectUpdate} = dmt_object:update_object(Object, Changes),
            UpdatesAcc1 = update_referenced_objects(Worker, Changes, ObjectUpdate, UpdatesAcc),
            {Worker, InsertsAcc, UpdatesAcc1#{Ref => ObjectUpdate}, [Ref | UpdatedObjectsAcc]};
        {remove, #domain_conf_v2_RemoveOp{ref = Ref}} ->
            {ok, OG} = get_original_object_changes(Worker, UpdatesAcc, Ref),
            #{references := OriginalReferences} = OG,
            UpdatesAcc1 = update_objects_removed_refs(Worker, Ref, OriginalReferences, UpdatesAcc),

            NewObjectState = dmt_object:remove_object(OG),
            {Worker, InsertsAcc, UpdatesAcc1#{Ref => NewObjectState}, [Ref | UpdatedObjectsAcc]}
    end.

update_referenced_objects(Worker, OriginalObjectChanges, ObjectChanges, Updates) ->
    #{
        id := ObjectID,
        type := ObjectType,
        references := OriginalReferences
    } = OriginalObjectChanges,
    ObjectRef = {ObjectType, ObjectID},
    #{references := NewVersionReferences} = ObjectChanges,
    ORS = ordsets:from_list(OriginalReferences),
    NVRS = ordsets:from_list(NewVersionReferences),
    AddedRefs = ordsets:subtract(NVRS, ORS),
    RemovedRefs = ordsets:subtract(ORS, NVRS),

    Updates1 = update_objects_added_refs(Worker, ObjectRef, AddedRefs, Updates),
    update_objects_removed_refs(Worker, ObjectRef, RemovedRefs, Updates1).

update_objects_added_refs(Worker, ObjectID, AddedRefs, Updates) ->
    lists:foldl(
        fun(Ref, Acc) ->
            {ok, OG} = get_referenced_object_changes(Worker, Acc, Ref, ObjectID),
            #{
                referenced_by := RefdBy0
            } = OG,
            Acc#{
                Ref =>
                    OG#{
                        referenced_by => [ObjectID | RefdBy0]
                    }
            }
        end,
        Updates,
        AddedRefs
    ).

update_objects_removed_refs(Worker, ObjectID, RemovedRefs, Updates) ->
    lists:foldl(
        fun(Ref, Acc) ->
            {ok, OG} = get_referenced_object_changes(Worker, Acc, Ref, ObjectID),
            #{
                referenced_by := RefdBy0
            } = OG,
            RefdBy1 = ordsets:from_list(RefdBy0),
            RefdBy2 = ordsets:del_element(ObjectID, RefdBy1),
            Acc#{
                Ref =>
                    OG#{
                        referenced_by => ordsets:to_list(RefdBy2)
                    }
            }
        end,
        Updates,
        RemovedRefs
    ).

get_referenced_object_changes(Worker, Updates, ReferencedRef, OriginalRef) ->
    try get_original_object_changes(Worker, Updates, ReferencedRef) of
        {ok, Object} ->
            {ok, Object}
    catch
        throw:{error, {operation_error, {conflict, {object_not_found, Ref}}}} = Error ->
            logger:error(
                "get_referenced_object_changes ReferencedRef ~p OriginalRef ~p",
                [Ref, OriginalRef]
            ),
            throw(Error)
    end.

get_original_object_changes(Worker, Updates, Ref) ->
    case Updates of
        #{Ref := Object} ->
            {ok, Object};
        _ ->
            case get_latest_target_object(Worker, Ref) of
                {ok, Res} ->
                    {ok, Res};
                {error, {object_not_found, Ref}} ->
                    throw({error, {operation_error, {conflict, {object_not_found, Ref}}}})
            end
    end.

commit(Version, Operations, AuthorID) ->
    Result = epg_pool:transaction(
        default_pool,
        fun(Worker) ->
            try
                {_Worker, InsertObjects, UpdateObjects0, ChangedObjectIds} =
                    assemble_operations(Worker, Operations),
                ok = check_versions_sql(Worker, ChangedObjectIds, Version),
                NewVersion = get_new_version(Worker, AuthorID),
                PermanentIDsMaps = insert_objects(Worker, InsertObjects, NewVersion),
                UpdateObjects1 = replace_tmp_ids_in_updates(UpdateObjects0, PermanentIDsMaps),
                ok = update_objects(Worker, UpdateObjects1, NewVersion),
                NewObjects = lists:map(
                    fun(#{data := Data}) ->
                        Data
                    end,
                    get_target_objects(Worker, maps:values(PermanentIDsMaps), NewVersion)
                ),
                {ok, NewVersion, NewObjects}
            catch
                Class:ExceptionPattern:Stacktrace ->
                    logger:error(
                        "Class:ExceptionPattern:Stacktrace ~p:~p:~p~n",
                        [Class, ExceptionPattern, Stacktrace]
                    ),
                    ExceptionPattern
            end
        end
    ),
    case Result of
        {ok, ResVersion, NewObjects} ->
            {ok, ResVersion, NewObjects};
        {error, {error, error, _, conflict_detected, Msg, _}} ->
            {error, {conflict, Msg}};
        {rollback, {error, {conflict, _} = Error}} ->
            {error, {operation_error, Error}};
        {rollback, {error, {invalid, _} = Error}} ->
            {error, {operation_error, Error}};
        {error, Error} ->
            {error, Error}
    end.

replace_tmp_ids_in_updates(UpdateObjects, PermanentIDsMaps) ->
    maps:map(
        fun(_ID, UpdateObject) ->
            #{
                referenced_by := ReferencedBy
            } = UpdateObject,
            NewReferencedBy = replace_referenced_by_ids(ReferencedBy, PermanentIDsMaps),
            UpdateObject#{
                referenced_by => NewReferencedBy
            }
        end,
        UpdateObjects
    ).

replace_referenced_by_ids(ReferencedBy, PermanentIDsMaps) ->
    lists:map(
        fun(Ref) ->
            case Ref of
                {temporary, TmpID} ->
                    maps:get(TmpID, PermanentIDsMaps);
                _ ->
                    Ref
            end
        end,
        ReferencedBy
    ).

check_versions_sql(Worker, ChangedObjectIds, Version) ->
    lists:foreach(
        fun(ChangedObjectId) ->
            ChangedObjectId0 = dmt_mapper:to_string(ChangedObjectId),
            case dmt_database:get_object_latest_version(Worker, ChangedObjectId0) of
                {ok, MostRecentVersion} when MostRecentVersion > Version ->
                    throw({object_update_too_old, {ChangedObjectId, MostRecentVersion}});
                {ok, _MostRecentVersion} ->
                    ok;
                {error, not_found} ->
                    {error, {unknown_object_update, ChangedObjectId}};
                {error, Error} ->
                    throw(Error)
            end
        end,
        ChangedObjectIds
    ),
    ok.

get_new_version(Worker, AuthorID) ->
    case dmt_database:get_new_version(Worker, AuthorID) of
        {ok, NewVersion} ->
            NewVersion;
        {error, Reason} ->
            throw({error, Reason})
    end.

insert_objects(Worker, InsertObjects, Version) ->
    lists:foldl(
        fun(InsertObject, Acc) ->
            #{
                tmp_id := TmpID,
                type := Type,
                forced_id := ForcedID,
                references := References,
                data := Data0
            } = InsertObject,
            Ref = get_insert_object_id(Worker, ForcedID, Type),
            Data1 = give_data_id(Data0, Ref),
            Ref = insert_object(Worker, Type, Ref, Version, References, Data1),
            Acc#{TmpID => Ref}
        end,
        #{},
        InsertObjects
    ).

insert_object(Worker, Type, ID0, Version, References0, Data0) ->
    ID1 = dmt_mapper:to_string(ID0),
    Data1 = dmt_mapper:to_string(Data0),
    References1 = lists:map(fun dmt_mapper:to_string/1, References0),

    case dmt_database:insert_object(Worker, ID1, Type, Version, References1, Data1) of
        ok ->
            ID0;
        {error, Reason} ->
            throw({error, Reason})
    end.

give_data_id({Tag, Data}, Ref) ->
    {struct, union, DomainObjects} = dmsl_domain_thrift:struct_info('DomainObject'),
    {value, {_, _, {_, _, {_, ObjectName}}, Tag, _}} = lists:search(
        fun({_, _, _, T, _}) ->
            case T of
                Tag ->
                    true;
                _ ->
                    false
            end
        end,
        DomainObjects
    ),
    RecordName = dmsl_domain_thrift:record_name(ObjectName),
    {_, _, [
        FirstField,
        SecondField
    ]} = dmsl_domain_thrift:struct_info(ObjectName),
    First = get_object_field(FirstField, Data, Ref),
    Second = get_object_field(SecondField, Data, Ref),
    {Tag, {RecordName, First, Second}}.

get_object_field({_, _, _, ref, _}, _Data, {_Type, Ref}) ->
    Ref;
get_object_field({_, _, _, data, _}, Data, _Ref) ->
    Data.

update_objects(Worker, UpdateObjects, Version) ->
    maps:foreach(
        fun(Ref, UpdateObject) ->
            #{
                id := Ref,
                type := Type,
                references := References,
                referenced_by := ReferencedBy,
                data := Data,
                is_active := IsActive
            } = UpdateObject,
            ok = update_object(Worker, Type, Ref, References, ReferencedBy, IsActive, Data, Version)
        end,
        UpdateObjects
    ).

update_object(Worker, Type, ID0, References0, ReferencedBy0, IsActive, Data0, Version) ->
    Data1 = dmt_mapper:to_string(Data0),
    ID1 = dmt_mapper:to_string(ID0),
    References1 = lists:map(fun dmt_mapper:to_string/1, References0),
    ReferencedBy1 = lists:map(fun dmt_mapper:to_string/1, ReferencedBy0),

    case dmt_database:update_object(Worker, ID1, Type, Version, References1, ReferencedBy1, Data1, IsActive) of
        ok ->
            ok;
        {error, Reason} ->
            throw({error, Reason})
    end.

get_insert_object_id(Worker, undefined, Type) ->
    %%  Check if sequence column exists in table
    %%  -- if it doesn't, then raise exception
    case dmt_database:get_next_sequence(Worker, Type) of
        {ok, NewID} ->
            % TODO need to do it without hardcode
            {Type, dmt_object_id:get_numerical_object_id(Type, NewID)};
        {error, sequence_not_enabled} ->
            throw({error, {object_type_requires_forced_id, Type}});
        {error, Reason} ->
            throw({error, Reason})
    end;
get_insert_object_id(Worker, Ref, _Type) ->
    Ref0 = dmt_mapper:to_string(Ref),
    case dmt_database:check_if_object_id_active(Worker, Ref0) of
        true ->
            throw({error, {conflict, {forced_id_exists, Ref}}});
        false ->
            Ref;
        {error, Reason} ->
            throw({error, Reason})
    end.

% get_target_objects(Refs, Version) ->
%     get_target_objects(default_pool, Refs, Version).

get_target_objects(Worker, Refs, Version) ->
    lists:map(
        fun(Ref) ->
            {ok, Obj} = get_target_object(Worker, Ref, Version),
            Obj
        end,
        Refs
    ).

get_target_object(Worker, Ref, Version) ->
    Ref0 = dmt_mapper:to_string(Ref),
    case dmt_database:check_version_exists(Worker, Version) of
        true ->
            case dmt_database:get_object(Worker, Ref0, Version) of
                {ok, Object} ->
                    add_created_by_to_object(Worker, Object);
                {error, not_found} ->
                    {error, {object_not_found, Ref}};
                {error, Error} ->
                    throw({error, Error})
            end;
        false ->
            {error, version_not_found};
        {error, Error} ->
            throw({error, Error})
    end.

add_created_by_to_object(Worker, Object) ->
    #{version := Version} = Object,
    {ok, AuthorID} = dmt_database:get_version_creator(Worker, Version),
    {ok, Author} = dmt_author:get(AuthorID),
    {ok, Object#{created_by => Author}}.

get_latest_target_object(Worker, Ref) ->
    Ref0 = dmt_mapper:to_string(Ref),

    case dmt_database:get_latest_object(Worker, Ref0) of
        {ok, LatestObject} ->
            {ok, LatestObject};
        {error, not_found} ->
            {error, {object_not_found, Ref}};
        {error, Error} ->
            throw({error, Error})
    end.

get_object_ref({Type, {_Object, ID, _Data}}) ->
    {ok, {Type, ID}};
get_object_ref(Obj) ->
    {error, {is_not_domain_object, Obj}}.
