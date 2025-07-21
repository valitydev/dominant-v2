-module(dmt_repository).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

%% API

-export([commit/3]).
-export([get_object/3]).
-export([get_objects/3]).
-export([get_snapshot/2]).
-export([get_latest_version/0]).
-export([get_object_history/2]).
-export([get_all_objects_history/1]).
-export([search_objects/1]).
-export([search_full_objects/1]).
-export([filter_search_results/1]).

%%

get_object(Worker, {version, V}, ObjectRef) ->
    case get_target_object(Worker, ObjectRef, V) of
        {ok, #{data := Data} = Object} ->
            % io:format("get_object Data ~p~n", [Data]),
            {ok, #domain_conf_v2_VersionedObject{
                info = marshall_to_object_info(Object),
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
                    changed_at = CreatedAt,
                    changed_by = CreatedBy
                },
                object = Data
            }};
        {error, Reason} ->
            {error, Reason}
    end.

get_objects(Worker, {version, V}, ObjectRefs) ->
    case dmt_database:check_version_exists(Worker, V) of
        true ->
            % Convert references to strings
            StringRefs = lists:map(fun dmt_mapper:to_string/1, ObjectRefs),
            % Get objects from database
            case dmt_database:get_objects(Worker, StringRefs, V) of
                {ok, Objects0} ->
                    Objects1 = sort_objects_by_ids(Objects0, ObjectRefs),
                    % Add created_by information to objects
                    {ok, EnrichedObjects} = add_created_by_to_objects(Worker, Objects1),
                    % Map each object to VersionedObject format
                    VersionedObjects = [
                        #domain_conf_v2_VersionedObject{
                            info = marshall_to_object_info(Object),
                            object = maps:get(data, Object)
                        }
                     || Object <- EnrichedObjects
                    ],
                    {ok, VersionedObjects};
                {error, Reason} ->
                    {error, Reason}
            end;
        false ->
            {error, version_not_found}
    end;
get_objects(Worker, {head, #domain_conf_v2_Head{}}, ObjectRefs) ->
    % For head, we need to get the latest version first
    case dmt_database:get_latest_version(Worker) of
        {ok, LatestVersion} ->
            get_objects(Worker, {version, LatestVersion}, ObjectRefs);
        {error, Reason} ->
            {error, Reason}
    end.

get_snapshot(Worker, {head, #domain_conf_v2_Head{}}) ->
    case dmt_database:get_latest_version(Worker) of
        {ok, LatestVersion} ->
            get_snapshot(Worker, {version, LatestVersion});
        {error, Reason} ->
            {error, Reason}
    end;
get_snapshot(Worker, {version, Version}) ->
    case dmt_database:check_version_exists(Worker, Version) of
        true ->
            case dmt_database:get_all_objects(Worker, Version) of
                {ok, Objects} ->
                    {ok, #{
                        created_at := CreatedAt,
                        created_by := AuthorID
                    }} = dmt_database:get_version(Worker, Version),
                    {ok, Author} = dmt_author:get(AuthorID),
                    Domain = #{K => V || #{id := K, data := V} <- Objects},
                    {ok, #domain_conf_v2_Snapshot{
                        version = Version,
                        domain = Domain,
                        created_at = dmt_mapper:datetime_to_binary(CreatedAt),
                        changed_by = Author
                    }};
                {error, Reason} ->
                    {error, Reason}
            end;
        false ->
            {error, version_not_found}
    end.

sort_objects_by_ids(Objects, IDs) ->
    % Create a map of ID -> Object for easier lookup
    ObjectsMap = maps:from_list([{maps:get(id, Obj), Obj} || Obj <- Objects]),

    % Use list comprehension to order objects according to input IDs
    % Skip IDs that don't have corresponding objects
    [
        maps:get(ID, ObjectsMap, undefined)
     || ID <- IDs,
        maps:is_key(ID, ObjectsMap)
    ].

get_object_history(ObjectRef, RequestParams) ->
    #domain_conf_v2_RequestParams{
        limit = Limit,
        continuation_token = Offset0
    } = RequestParams,
    Offset1 = maybe_from_string(Offset0, 0),
    maybe
        StringRef = dmt_mapper:to_string(ObjectRef),
        {ok, Objects0, NewOffset} ?=
            dmt_database:get_object_history(default_pool, StringRef, Limit, Offset1),
        {ok, Objects1} = add_created_by_to_objects(default_pool, Objects0),
        Result = #domain_conf_v2_ObjectVersionsResponse{
            result = [
                #domain_conf_v2_LimitedVersionedObject{
                    info = marshall_to_object_info(Object),
                    ref = maps:get(id, Object),
                    name = dmt_domain:maybe_get_domain_object_data_field(name, Data),
                    description = dmt_domain:maybe_get_domain_object_data_field(description, Data)
                }
             || #{data := Data} = Object <- Objects1
            ],
            total_count = length(Objects1),
            continuation_token = maybe_to_string(NewOffset, undefined)
        },
        {ok, Result}
    else
        {error, not_found} ->
            {error, not_found};
        {error, Reason} ->
            {error, Reason}
    end.

% Done this way to keep hierarchy of calls
get_latest_version() ->
    dmt_database:get_latest_version(default_pool).

get_all_objects_history(Request) ->
    #domain_conf_v2_RequestParams{
        limit = Limit,
        continuation_token = Offset0
    } = Request,
    Offset1 = maybe_from_string(Offset0, 0),
    maybe
        {ok, Objects0, NewOffset} ?=
            dmt_database:get_all_objects_history(default_pool, Limit, Offset1),
        {ok, Objects1} ?= add_created_by_to_objects(default_pool, Objects0),
        Result = #domain_conf_v2_ObjectVersionsResponse{
            result = [
                #domain_conf_v2_LimitedVersionedObject{
                    info = marshall_to_object_info(Object),
                    ref = maps:get(id, Object),
                    name = dmt_domain:maybe_get_domain_object_data_field(name, Data),
                    description = dmt_domain:maybe_get_domain_object_data_field(description, Data)
                }
             || #{data := Data} = Object <- Objects1
            ],
            total_count = length(Objects1),
            continuation_token = maybe_to_string(NewOffset, undefined)
        },
        {ok, Result}
    else
        {error, Reason} ->
            {error, Reason}
    end.

maybe_to_string(undefined, Default) -> Default;
maybe_to_string(Value, _) -> dmt_mapper:to_string(Value).

maybe_from_string(undefined, Default) -> Default;
maybe_from_string(Value, _) -> dmt_mapper:from_string(Value).

marshall_to_object_info(Object) ->
    #domain_conf_v2_VersionedObjectInfo{
        version = maps:get(version, Object),
        changed_at = maps:get(created_at, Object),
        changed_by = maps:get(created_by, Object)
    }.

search_objects(Request) ->
    #domain_conf_v2_SearchRequestParams{
        query = Query,
        version = Version,
        limit = Limit,
        type = Type,
        continuation_token = ContinuationToken
    } = Request,

    maybe
        ok ?= maybe_check_entity_type_exists(Type),
        Version2 = get_version(default_pool, Version),
        {ok, {Objects0, NewOffset}} = dmt_database:search_objects(
            default_pool,
            Query,
            Version2,
            Type,
            Limit,
            maybe_from_string(ContinuationToken, 0)
        ),
        Objects1 = filter_search_results(Objects0),
        {ok, Objects2} = add_created_by_to_objects(default_pool, Objects1),
        Result = #domain_conf_v2_SearchResponse{
            result = [
                #domain_conf_v2_LimitedVersionedObject{
                    info = marshall_to_object_info(Object),
                    ref = maps:get(id, Object),
                    name = dmt_domain:maybe_get_domain_object_data_field(name, Data),
                    description = dmt_domain:maybe_get_domain_object_data_field(description, Data)
                }
             || #{data := Data} = Object <- Objects2
            ],
            total_count = length(Objects2),
            continuation_token = maybe_to_string(NewOffset, undefined)
        },
        {ok, Result}
    else
        {error, object_type_not_found} ->
            {error, object_type_not_found};
        {error, Reason} ->
            {error, Reason}
    end.

search_full_objects(Request) ->
    #domain_conf_v2_SearchRequestParams{
        query = Query,
        version = Version,
        limit = Limit,
        type = Type,
        continuation_token = ContinuationToken
    } = Request,

    maybe
        ok ?= maybe_check_entity_type_exists(Type),
        Version2 = get_version(default_pool, Version),
        {ok, {Objects0, NewOffset}} = dmt_database:search_objects(
            default_pool,
            Query,
            Version2,
            Type,
            Limit,
            maybe_from_string(ContinuationToken, 0)
        ),
        Objects1 = filter_search_results(Objects0),
        {ok, Objects2} = add_created_by_to_objects(default_pool, Objects1),
        Result = #domain_conf_v2_SearchFullResponse{
            result = [
                #domain_conf_v2_VersionedObject{
                    info = marshall_to_object_info(Object),
                    object = Data
                }
             || #{data := Data} = Object <- Objects2
            ],
            total_count = length(Objects2),
            continuation_token = maybe_to_string(NewOffset, undefined)
        },
        {ok, Result}
    else
        {error, object_type_not_found} ->
            {error, object_type_not_found};
        {error, Reason} ->
            {error, Reason}
    end.

filter_search_results(Objects) ->
    lists:filter(
        fun(Object) ->
            ID = maps:get(id, Object),
            Data = maps:get(data, Object),
            case
                {
                    dmt_thrift_validator:validate_reference(ID),
                    dmt_thrift_validator:validate_domain_object(Data)
                }
            of
                {ok, ok} ->
                    true;
                {ErrorID, ErrorData} ->
                    logger:error("Search validation error ID ~p ~p", [ID, ErrorID]),
                    logger:error("Search validation error Data ~p ~p", [Data, ErrorData]),
                    false
            end
        end,
        Objects
    ).

maybe_check_entity_type_exists(undefined) -> ok;
maybe_check_entity_type_exists(Type) -> dmt_database:check_entity_type_exists(default_pool, Type).

assemble_operations(Worker, Operations) ->
    {_, Inserts, Updates1, UpdatedObjects, TmpIDOperations} = lists:foldl(
        fun assemble_operations_parse/2,
        {Worker, [], #{}, [], []},
        Operations
    ),
    {_, Updates2} = lists:foldl(
        fun assemble_operations_refs_insert/2,
        {Worker, Updates1},
        Inserts
    ),
    {_, Updates3} = lists:foldl(
        fun assemble_operations_refs_update/2,
        {Worker, Updates2},
        maps:values(Updates2)
    ),
    {Worker, Inserts, Updates3, UpdatedObjects, TmpIDOperations}.

assemble_operations_parse(
    Operation,
    {Worker, InsertsAcc, UpdatesAcc, UpdatedObjectsAcc, OperationsAcc}
) ->
    case Operation of
        {insert, #domain_conf_v2_InsertOp{} = InsertOp} ->
            {ok, NewObject} = dmt_object:new_object(InsertOp),
            FinalOperation = {Operation, {tmp_id, maps:get(tmp_id, NewObject)}},

            {
                Worker,
                [NewObject | InsertsAcc],
                UpdatesAcc,
                UpdatedObjectsAcc,
                [FinalOperation | OperationsAcc]
            };
        {update, #domain_conf_v2_UpdateOp{object = Object}} ->
            {ok, Ref} = get_object_ref(Object),
            {ok, Changes} = get_original_object_changes(Worker, UpdatesAcc, Ref),
            {ok, ObjectUpdate} = dmt_object:update_object(Object, Changes),
            {Worker, InsertsAcc, UpdatesAcc#{Ref => ObjectUpdate}, [Ref | UpdatedObjectsAcc], [
                Operation | OperationsAcc
            ]};
        {remove, #domain_conf_v2_RemoveOp{ref = Ref}} ->
            {ok, OG} = get_original_object_changes(Worker, UpdatesAcc, Ref),

            NewObjectState = dmt_object:remove_object(OG),
            {Worker, InsertsAcc, UpdatesAcc#{Ref => NewObjectState}, [Ref | UpdatedObjectsAcc], [
                Operation | OperationsAcc
            ]}
    end.

assemble_operations_refs_insert(
    Insert,
    {Worker, UpdatesAcc}
) ->
    #{
        tmp_id := TmpID,
        references := Refers
    } = Insert,
    Updates1 = update_objects_added_refs(Worker, {temporary, TmpID}, Refers, UpdatesAcc),
    {Worker, Updates1}.

assemble_operations_refs_update(
    Update,
    {Worker, UpdatesAcc}
) ->
    #{
        id := Ref,
        is_active := IsActive
    } = Update,
    {ok, Changes} = get_original_object_changes(Worker, UpdatesAcc, Ref),
    case IsActive of
        true ->
            UpdatesAcc1 = update_referenced_objects(Worker, Changes, Update, UpdatesAcc),
            {Worker, UpdatesAcc1};
        false ->
            #{references := OriginalReferences} = Changes,
            UpdatesAcc1 = update_objects_removed_refs(Worker, Ref, OriginalReferences, UpdatesAcc),
            {Worker, UpdatesAcc1}
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
                {_Worker, InsertObjects, UpdateObjects0, ChangedObjectIds, TmpIDOperations} =
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

                %% Publish Kafka event after successful transaction
                ok = publish_commit_event(NewVersion, TmpIDOperations, PermanentIDsMaps, AuthorID),
                {ok, NewVersion, NewObjects, AuthorID}
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
        {ok, ResVersion, NewObjects, AuthorID} ->
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
    SearchVector = dmt_mapper:extract_searchable_text_from_term(Data0),

    case dmt_database:insert_object(Worker, ID1, Type, Version, References1, Data1, SearchVector) of
        ok ->
            ID0;
        {error, Reason} ->
            logger:error(
                "insert_object Type: ~p, ID: ~p, Version: ~p, References: ~p, Data: ~p, SearchVector: ~p",
                [Type, ID0, Version, References1, Data1, SearchVector]
            ),
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
    SearchVector = dmt_mapper:extract_searchable_text_from_term(Data0),

    case
        dmt_database:update_object(
            Worker,
            ID1,
            Type,
            Version,
            References1,
            ReferencedBy1,
            Data1,
            SearchVector,
            IsActive
        )
    of
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
            throw({error, {operation_error, {conflict, {forced_id_exists, Ref}}}});
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
                    {error, {object_not_found, Ref}}
            end;
        false ->
            {error, version_not_found}
    end.

add_created_by_to_objects(Worker, Objects) ->
    Versions = lists:uniq([Version || #{version := Version} <- Objects]),
    AuthorsOfVersions =
        #{
            Version => Author
         || Version <- Versions,
            {ok, AuthorID} <- [dmt_database:get_version_creator(Worker, Version)],
            {ok, Author} <- [dmt_author:get(AuthorID)]
        },
    EnrichedObjects = [
        Object#{
            created_by => maps:get(Version, AuthorsOfVersions, undefined)
        }
     || #{version := Version} = Object <- Objects
    ],
    {ok, EnrichedObjects}.

add_created_by_to_object(Worker, Object) ->
    #{version := Version} = Object,
    {ok, AuthorID} = dmt_database:get_version_creator(Worker, Version),
    {ok, Author} = dmt_author:get(AuthorID),
    {ok, Object#{created_by => Author}}.

get_latest_target_object(Worker, Ref) ->
    Ref0 = dmt_mapper:to_string(Ref),

    case dmt_database:get_latest_object(Worker, Ref0) of
        {ok, LatestObject} ->
            add_created_by_to_object(Worker, LatestObject);
        {error, not_found} ->
            {error, {object_not_found, Ref}}
    end.

get_version(Worker, undefined) ->
    {ok, LatestVersion} = dmt_database:get_latest_version(Worker),
    LatestVersion;
get_version(_Worker, Version) ->
    Version.

get_object_ref({Type, {_Object, ID, _Data}}) ->
    {ok, {Type, ID}}.

%% @doc Publish commit event to Kafka after successful transaction
-spec publish_commit_event(
    Version :: integer(), Operations :: list(), PermanentIDsMaps :: map(), AuthorID :: binary()
) -> ok.
publish_commit_event(Version, Operations, PermanentIDsMaps, AuthorID) ->
    try
        %% Get author information
        case dmt_author:get(AuthorID) of
            {ok, Author} ->
                %% Convert operations to final operations for HistoricalCommit
                FinalOps = convert_to_final_operations(Operations, PermanentIDsMaps),

                %% Create HistoricalCommit record
                HistoricalCommit = #domain_conf_v2_HistoricalCommit{
                    version = Version,
                    ops = FinalOps,
                    created_at = dmt_mapper:datetime_to_binary(
                        calendar:system_time_to_universal_time(
                            erlang:system_time(millisecond), millisecond
                        )
                    ),
                    changed_by = Author
                },

                %% Publish to Kafka
                case dmt_kafka_publisher:publish_commit_event(HistoricalCommit) of
                    ok ->
                        logger:debug("Successfully published commit event for version ~p", [Version]);
                    {error, Reason} ->
                        logger:warning("Failed to publish commit event for version ~p: ~p", [
                            Version, Reason
                        ])
                end;
            {error, Reason} ->
                logger:warning("Failed to get author ~p for Kafka event: ~p", [AuthorID, Reason])
        end
    catch
        Class:Error:Stacktrace ->
            logger:error("Exception in publish_commit_event: ~p:~p~n~p", [Class, Error, Stacktrace])
    end.

%% @doc Convert operations to final operations for HistoricalCommit
-spec convert_to_final_operations(list(), map()) -> list().
convert_to_final_operations(Operations, PermanentIDsMaps) ->
    lists:map(
        fun(Operation) ->
            case Operation of
                {{insert, InsertOp}, {tmp_id, TmpID}} ->
                    ReflessObject = InsertOp#domain_conf_v2_InsertOp.object,

                    FinalRef = maps:get(TmpID, PermanentIDsMaps),

                    {insert, #domain_conf_v2_FinalInsertOp{
                        object = give_data_id(ReflessObject, FinalRef)
                    }};
                _ ->
                    Operation
            end
        end,
        Operations
    ).
