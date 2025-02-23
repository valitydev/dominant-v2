-module(dmt_repository).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("epgsql/include/epgsql.hrl").

%% API

-export([commit/3]).
-export([get_object/3]).
-export([get_latest_version/0]).

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

get_latest_version() ->
    Query1 =
        """
        SELECT MAX(version) FROM version;
        """,
    case epg_pool:query(default_pool, Query1) of
        {ok, _Columns, [{null}]} ->
            {ok, 0};
        {ok, _Columns, [{Version}]} ->
            {ok, Version};
        {error, Reason} ->
            {error, Reason}
    end.

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
            Ref = get_object_ref(Object),
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
                    {Type, _} = Ref,
                    {ok, Res#{
                        type => Type
                    }};
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
                {ok, NewVersion, maps:values(PermanentIDsMaps)}
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
        {ok, ResVersion, NewObjectsIDs} ->
            NewObjects = lists:map(
                fun(#{data := Data}) ->
                    Data
                end,
                get_target_objects(NewObjectsIDs, ResVersion)
            ),
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
        fun({ChangedObjectType, ChangedObjectRef0} = ChangedObjectId) ->
            ChangedObjectRef1 = to_string(ChangedObjectRef0),
            Query0 =
                io_lib:format("""
                SELECT id, version
                FROM ~p
                WHERE id = $1
                ORDER BY version DESC
                LIMIT 1
                """, [ChangedObjectType]),
            case epg_pool:query(Worker, Query0, [ChangedObjectRef1]) of
                {ok, _Columns, []} ->
                    throw({unknown_object_update, ChangedObjectId});
                {ok, _Columns, [{ChangedObjectRef, MostRecentVersion}]} when MostRecentVersion > Version ->
                    throw({object_update_too_old, {ChangedObjectRef, MostRecentVersion}});
                {ok, _Columns, [{_ChangedObjectRef, _MostRecentVersion}]} ->
                    ok;
                {error, Reason} ->
                    throw({error, Reason})
            end
        end,
        ChangedObjectIds
    ),
    ok.

get_new_version(Worker, AuthorID) ->
    Query1 =
        """
        INSERT INTO version (CREATED_BY)
        VALUES ($1::uuid) RETURNING version;
        """,
    case epg_pool:query(Worker, Query1, [AuthorID]) of
        {ok, 1, _Columns, [{NewVersion}]} ->
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
            {ID, Sequence} = get_insert_object_id(Worker, ForcedID, Type),
            Data1 = give_data_id(Data0, ID),
            ID = insert_object(Worker, Type, ID, Sequence, Version, References, Data1),
            Acc#{TmpID => {Type, ID}}
        end,
        #{},
        InsertObjects
    ).

insert_object(Worker, Type, ID0, Sequence, Version, References0, Data0) ->
    ID1 = to_string(ID0),
    Data1 = to_string(Data0),
    References1 = lists:map(fun to_string/1, References0),
    Params0 = [Version, References1, [], Data1],
    {Query, Params1} =
        case check_if_force_id_required(Worker, Type) of
            true ->
                Query0 =
                    io_lib:format("""
                    INSERT INTO ~p (id, version, references_to, referenced_by, data, is_active)
                        VALUES ($1, $2, $3, $4, $5, TRUE);
                    """, [Type]),
                {Query0, [ID1 | Params0]};
            false ->
                Query1 =
                    io_lib:format("""
                    INSERT INTO ~p (id, sequence, version, references_to, referenced_by, data, is_active)
                        VALUES ($1, $2, $3, $4, $5, $6, TRUE);
                    """, [Type]),
                {Query1, [ID1, Sequence | Params0]}
        end,
    case epg_pool:query(Worker, Query, Params1) of
        {ok, 1} ->
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

get_object_field({_, _, _, ref, _}, _Data, Ref) ->
    Ref;
get_object_field({_, _, _, data, _}, Data, _Ref) ->
    Data.

update_objects(Worker, UpdateObjects, Version) ->
    maps:foreach(
        fun({_, ID}, UpdateObject) ->
            #{
                id := ID,
                type := Type,
                references := References,
                referenced_by := ReferencedBy,
                data := Data,
                is_active := IsActive
            } = UpdateObject,
            ok = update_object(Worker, Type, ID, References, ReferencedBy, IsActive, Data, Version)
        end,
        UpdateObjects
    ).

update_object(Worker, Type, ID0, References0, ReferencedBy0, IsActive, Data0, Version) ->
    Data1 = to_string(Data0),
    ID1 = to_string(ID0),
    References1 = lists:map(fun to_string/1, References0),
    ReferencedBy1 = lists:map(fun to_string/1, ReferencedBy0),
    Query =
        io_lib:format("""
        INSERT INTO ~p
        (id, version, references_to, referenced_by, data, is_active)
            VALUES ($1, $2, $3, $4, $5, $6);
        """, [Type]),
    Params = [ID1, Version, References1, ReferencedBy1, Data1, IsActive],
    case epg_pool:query(Worker, Query, Params) of
        {ok, 1} ->
            ok;
        {error, Reason} ->
            throw({error, Reason})
    end.

get_insert_object_id(Worker, undefined, Type) ->
    %%  Check if sequence column exists in table
    %%  -- if it doesn't, then raise exception
    case check_if_force_id_required(Worker, Type) of
        true ->
            throw({error, {object_type_requires_forced_id, Type}});
        false ->
            {ok, LastSequenceInType} = get_last_sequence(Worker, Type),
            case get_new_object_id(Worker, LastSequenceInType, Type) of
                {undefined, Seq} ->
                    throw({error, {free_id_not_found, Seq, Type}});
                {NewID, NewSequence} ->
                    {NewID, NewSequence}
            end
    end;
get_insert_object_id(Worker, {Type, ForcedID}, Type) ->
    case check_if_id_exists(Worker, ForcedID, Type) of
        true ->
            case check_if_object_active(Worker, ForcedID, Type) of
                true ->
                    throw({error, {conflict, {forced_id_exists, {Type, ForcedID}}}});
                false ->
                    {ForcedID, null}
            end;
        false ->
            {ForcedID, null}
    end.

check_if_force_id_required(Worker, Type) ->
    Query = """
    SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $1 AND column_name = 'sequence';
    """,
    case epg_pool:query(Worker, Query, [Type]) of
        {ok, _Columns, []} ->
            true;
        {ok, _Columns, Rows} ->
            has_sequence_column(Rows);
        {error, Reason} ->
            throw({error, Reason})
    end.

has_sequence_column(Rows) ->
    lists:all(
        fun(Row) ->
            case Row of
                {<<"sequence">>} ->
                    false;
                _ ->
                    true
            end
        end,
        Rows
    ).

get_last_sequence(Worker, Type) ->
    Query = io_lib:format("""
    SELECT MAX(sequence)
    FROM ~p;
    """, [Type]),
    case epg_pool:query(Worker, Query) of
        {ok, _Columns, [{null}]} ->
            {ok, 0};
        {ok, _Columns, [{LastID}]} ->
            {ok, LastID};
        {error, Reason} ->
            throw({error, Reason})
    end.

get_new_object_id(Worker, LastSequenceInType, Type) ->
    genlib_list:foldl_while(
        fun(_I, {ID, Sequence}) ->
            NextSequence = Sequence + 1,
            NewID = dmt_object_id:get_numerical_object_id(Type, NextSequence),
            case check_if_id_exists(Worker, NewID, Type) of
                false ->
                    {halt, {NewID, NextSequence}};
                true ->
                    {cont, {ID, NextSequence}}
            end
        end,
        {undefined, LastSequenceInType},
        lists:seq(1, 100)
    ).

check_if_id_exists(Worker, ID0, Type0) ->
    Query = io_lib:format("""
    SELECT id
    FROM ~p
    WHERE id = $1
    LIMIT 1;
    """, [Type0]),
    ID1 = to_string(ID0),
    case epg_pool:query(Worker, Query, [ID1]) of
        {ok, _Columns, []} ->
            false;
        {ok, _Columns, [{ID1}]} ->
            true;
        {error, Reason} ->
            throw({error, Reason})
    end.

check_if_object_active(Worker, ID0, Type0) ->
    Query = io_lib:format("""
    SELECT is_active
    FROM ~p
    WHERE id = $1
    ORDER BY version DESC
    LIMIT 1;
    """, [Type0]),
    ID1 = to_string(ID0),
    case epg_pool:query(Worker, Query, [ID1]) of
        {ok, _Columns, []} ->
            throw({object_dissapeared, {Type0, ID0}});
        {ok, _Columns, [{IsActive}]} ->
            IsActive;
        {error, Reason} ->
            throw({error, Reason})
    end.

get_target_objects(Refs, Version) ->
    get_target_objects(default_pool, Refs, Version).

get_target_objects(Worker, Refs, Version) ->
    lists:map(
        fun(Ref) ->
            {ok, Obj} = get_target_object(Worker, Ref, Version),
            Obj
        end,
        Refs
    ).

get_target_object(Worker, Ref, Version) ->
    % First check if the version exists
    case check_version_exists(Worker, Version) of
        {ok, exists} ->
            fetch_object(Worker, Ref, Version);
        {ok, not_exists} ->
            {error, version_not_found};
        Error ->
            Error
    end.

check_version_exists(Worker, Version) ->
    VersionRequest = """
    SELECT 1
    FROM version
    WHERE version = $1
    LIMIT 1
    """,
    case epg_pool:query(Worker, VersionRequest, [Version]) of
        {ok, _Columns, []} ->
            {ok, not_exists};
        {ok, _Columns, [_Row]} ->
            {ok, exists};
        Error ->
            Error
    end.

fetch_object(Worker, Ref, Version) ->
    {Type, ID} = Ref,
    ID0 = to_string(ID),
    Request = io_lib:format("""
    SELECT id,
           version,
           references_to,
           referenced_by,
           data,
           is_active,
           created_at
    FROM ~p
    WHERE id = $1 AND version <= $2
    ORDER BY version DESC
    LIMIT 1
    """, [Type]),
    case epg_pool:query(Worker, Request, [ID0, Version]) of
        {ok, _Columns, []} ->
            {error, {object_not_found, Ref}};
        {ok, Columns, Rows} ->
            [Result | _] = to_marshalled_maps(Columns, Rows),
            {ok, Result}
    end.

get_latest_target_object(Worker, Ref) ->
    {Type, ID} = Ref,
    ID0 = to_string(ID),
    Request = io_lib:format("""
    SELECT id,
           version,
           references_to,
           referenced_by,
           data,
           is_active,
           created_at
    FROM ~p
    WHERE id = $1
    ORDER BY version DESC
    LIMIT 1
    """, [Type]),
    case epg_pool:query(Worker, Request, [ID0]) of
        {ok, _Columns, []} ->
            {error, {object_not_found, Ref}};
        {ok, Columns, Rows} ->
            [Result | _] = to_marshalled_maps(Columns, Rows),
            {ok, Result}
    end.

to_marshalled_maps(Columns, Rows) ->
    to_maps(Columns, Rows, fun marshall_object/1).

to_maps(Columns, Rows, TransformRowFun) ->
    ColNumbers = erlang:length(Columns),
    Seq = lists:seq(1, ColNumbers),
    lists:map(
        fun(Row) ->
            Data = lists:foldl(
                fun(Pos, Acc) ->
                    #column{name = Field, type = Type} = lists:nth(Pos, Columns),
                    Acc#{Field => convert(Type, erlang:element(Pos, Row))}
                end,
                #{},
                Seq
            ),
            TransformRowFun(Data)
        end,
        Rows
    ).

%% for reference https://github.com/epgsql/epgsql#data-representation
convert(timestamp, Value) ->
    datetime_to_binary(Value);
convert(timestamptz, Value) ->
    datetime_to_binary(Value);
convert(_Type, Value) ->
    Value.

datetime_to_binary({Date, {Hour, Minute, Second}}) when is_float(Second) ->
    datetime_to_binary({Date, {Hour, Minute, trunc(Second)}});
datetime_to_binary(DateTime) ->
    UnixTime = genlib_time:daytime_to_unixtime(DateTime),
    genlib_rfc3339:format(UnixTime, second).

marshall_object(#{
    <<"id">> := ID,
    <<"version">> := Version,
    <<"references_to">> := ReferencesTo,
    <<"referenced_by">> := ReferencedBy,
    <<"data">> := Data,
    <<"created_at">> := CreatedAt,
    <<"is_active">> := IsActive
}) ->
    dmt_object:just_object(
        from_string(ID),
        Version,
        lists:map(fun from_string/1, ReferencesTo),
        lists:map(fun from_string/1, ReferencedBy),
        from_string(Data),
        CreatedAt,
        IsActive
    ).

to_string(A0) ->
    A1 = term_to_binary(A0),
    base64:encode(A1).

from_string(B0) ->
    B1 = base64:decode(B0),
    binary_to_term(B1).

get_object_ref({Type, {_Object, ID, _Data}}) ->
    {ok, {Type, ID}};
get_object_ref(Obj) ->
    {error, {is_not_domain_object, Obj}}.
