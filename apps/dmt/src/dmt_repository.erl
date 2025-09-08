-module(dmt_repository).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

%% API

-export([commit/3]).
-export([get_object/3]).
-export([get_object_with_references/3]).
-export([get_objects/3]).
-export([get_snapshot/2]).
-export([get_latest_version/0]).
-export([get_object_history/2]).
-export([get_all_objects_history/1]).
-export([search_objects/1]).
-export([search_full_objects/1]).
-export([filter_search_results/1]).
-export([get_related_graph/1]).

%%

get_object(Worker, {version, V}, ObjectRef) ->
    case get_target_object(Worker, ObjectRef, V) of
        {ok, #{data := Data} = Object} ->
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

get_object_with_references(Worker, {version, V}, ObjectRef) ->
    case get_target_object(Worker, ObjectRef, V) of
        {ok, #{data := Data} = Object} ->
            ObjectRefString = dmt_mapper:to_string(ObjectRef),
            ReferencedByRefs = dmt_database:get_referenced_by(Worker, ObjectRefString, V),
            ReferencesToRefs = dmt_database:get_references_to(Worker, ObjectRefString, V),

            {ok, ReferencedBy} = get_objects(
                Worker, {version, V}, ordsets:from_list(ReferencedByRefs)
            ),
            {ok, ReferencesTo} = get_objects(
                Worker, {version, V}, ordsets:from_list(ReferencesToRefs)
            ),

            {ok, #domain_conf_v2_VersionedObjectWithReferences{
                object =
                    #domain_conf_v2_VersionedObject{
                        info = marshall_to_object_info(Object),
                        object = Data
                    },
                referenced_by = ordsets:from_list(ReferencedBy),
                references_to = ordsets:from_list(ReferencesTo)
            }};
        {error, Reason} ->
            {error, Reason}
    end;
get_object_with_references(Worker, {head, #domain_conf_v2_Head{}}, ObjectRef) ->
    {ok, Version} = dmt_database:get_latest_version(Worker),
    get_object_with_references(Worker, {version, Version}, ObjectRef).

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

get_related_graph(Request) ->
    #domain_conf_v2_RelatedGraphRequest{
        ref = ObjectRef,
        version = Version,
        type = Type,
        include_inbound = IncludeInbound,
        include_outbound = IncludeOutbound,
        depth = Depth
    } = Request,

    maybe
        Worker = default_pool,
        {ok, ResolvedVersion} ?= resolve_version_reference(Worker, Version),
        ok ?= validate_object_exists(Worker, ObjectRef, ResolvedVersion),
        % Use SQL-based graph traversal for better performance
        ObjectRefString = dmt_mapper:to_string(ObjectRef),
        {ok, {NodeMaps, EdgeMaps}} ?=
            dmt_database:get_related_graph(
                Worker,
                ObjectRefString,
                ResolvedVersion,
                Depth,
                IncludeInbound,
                IncludeOutbound,
                Type
            ),
        % Convert to Thrift structures
        {ok, NodesWithAuthors} = add_created_by_to_objects(Worker, NodeMaps),
        NodesResult = [
            #domain_conf_v2_LimitedVersionedObject{
                info = marshall_to_object_info(Node),
                ref = maps:get(id, Node),
                name = dmt_domain:maybe_get_domain_object_data_field(name, maps:get(data, Node)),
                description = dmt_domain:maybe_get_domain_object_data_field(
                    description, maps:get(data, Node)
                )
            }
         || Node <- NodesWithAuthors
        ],
        EdgesResult = [
            #domain_conf_v2_ReferenceEdge{
                source = maps:get(source_ref, Edge),
                target = maps:get(target_ref, Edge)
            }
         || Edge <- EdgeMaps
        ],
        Result = #domain_conf_v2_RelatedGraph{
            nodes = ordsets:from_list(NodesResult),
            edges = ordsets:from_list(EdgesResult)
        },
        {ok, Result}
    else
        {error, object_not_found} ->
            {error, object_not_found};
        {error, version_not_found} ->
            {error, version_not_found};
        {error, Reason} ->
            {error, Reason}
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

commit_operations(Worker, Operations, TargetVersion, NewVersion) ->
    {_, _, _, RelationsChanges, FinalOperations, NewObjects, RemovedObjectsReferences} = lists:foldl(
        fun commit_operation/2,
        {Worker, TargetVersion, NewVersion, #{}, [], [], []},
        Operations
    ),
    {RelationsChanges, FinalOperations, NewObjects, RemovedObjectsReferences}.

commit_operation(
    {insert, #domain_conf_v2_InsertOp{object = Object, force_ref = ForceRef}},
    {Worker, TargetVersion, NewVersion, RelationsChanges, FinalOperations, NewObjects, RemovedObjectsReferences}
) ->
    {Type, _} = Object,
    References = dmt_object_reference:refless_object_references(Object),
    Ref = get_insert_object_id(Worker, ForceRef, Type),
    Data1 = give_data_id(Object, Ref),
    Ref = insert_object(Worker, Type, Ref, NewVersion, Data1),
    FinalOperation = {insert, #domain_conf_v2_FinalInsertOp{object = Data1}},

    {
        Worker,
        TargetVersion,
        NewVersion,
        RelationsChanges#{Ref => References},
        [FinalOperation | FinalOperations],
        [Data1 | NewObjects],
        RemovedObjectsReferences
    };
commit_operation(
    {update, #domain_conf_v2_UpdateOp{object = Object}} = Operation,
    {Worker, TargetVersion, NewVersion, RelationsChanges, FinalOperations, NewObjects, RemovedObjectsReferences}
) ->
    {ok, {Type, _ID} = Ref} = get_object_ref(Object),

    ok = validate_latest_version(Worker, TargetVersion, Ref),

    References = dmt_object_reference:domain_object_references(Object),
    ok = update_object(Worker, Type, Ref, true, Object, NewVersion),
    {
        Worker,
        TargetVersion,
        NewVersion,
        RelationsChanges#{Ref => References},
        [Operation | FinalOperations],
        NewObjects,
        RemovedObjectsReferences
    };
commit_operation(
    {remove, #domain_conf_v2_RemoveOp{ref = Ref}} = Operation,
    {Worker, TargetVersion, NewVersion, RelationsChanges, FinalOperations, NewObjects, RemovedObjectsReferences}
) ->
    ok = validate_latest_version(Worker, TargetVersion, Ref),
    %% Validation of inbound references is deferred to post-relations stage
    %% to allow removing referencing and referenced entities in the same commit

    {Type, _} = Ref,
    {ok, #{data := Object}} = get_target_object(Worker, Ref, TargetVersion),
    ok = update_object(Worker, Type, Ref, false, Object, NewVersion),
    {
        Worker,
        TargetVersion,
        NewVersion,
        RelationsChanges#{Ref => []},
        [Operation | FinalOperations],
        NewObjects,
        [Ref | RemovedObjectsReferences]
    }.

insert_relation(Worker, OriginRef, Reference, NewVersion, IsActive) ->
    OriginRef1 = dmt_mapper:to_string(OriginRef),
    Reference1 = dmt_mapper:to_string(Reference),
    case dmt_database:insert_relations(Worker, OriginRef1, Reference1, NewVersion, IsActive) of
        ok ->
            ok;
        {error, {source_entity_not_found, EntityId}} ->
            throw({error, {invalid, {objects_not_exist, [{OriginRef, [EntityId]}]}}});
        {error, {target_entity_not_found, EntityId}} ->
            throw({error, {invalid, {objects_not_exist, [{EntityId, [OriginRef]}]}}});
        {error, {duplicate_relation, _}} ->
            % Duplicate relations are acceptable, continue
            ok;
        {error, Reason} ->
            throw({error, Reason})
    end.

commit_relations_changes(Worker, NewVersion, RelationsChanges) ->
    maps:foreach(
        fun(OriginRef, References) ->
            OriginRef1 = dmt_mapper:to_string(OriginRef),
            ExistingReferences = dmt_database:get_references_to(Worker, OriginRef1, NewVersion),

            ReferencesSet = ordsets:from_list(References),
            ExistingReferencesSet = ordsets:from_list(ExistingReferences),

            NewReferencesSet = ordsets:subtract(ReferencesSet, ExistingReferencesSet),
            RemovedReferencesSet = ordsets:subtract(ExistingReferencesSet, ReferencesSet),

            % Insert new relations
            ok = lists:foreach(
                fun(NewReference) ->
                    insert_relation(Worker, OriginRef, NewReference, NewVersion, true)
                end,
                ordsets:to_list(NewReferencesSet)
            ),

            % Insert removed relations
            ok = lists:foreach(
                fun(RemovedReference) ->
                    insert_relation(Worker, OriginRef, RemovedReference, NewVersion, false)
                end,
                ordsets:to_list(RemovedReferencesSet)
            )
        end,
        RelationsChanges
    ).

commit(Version, Operations, AuthorID) ->
    Result = epg_pool:transaction(
        default_pool,
        fun(Worker) ->
            try
                ok = validate_author_exists(Worker, AuthorID),
                NewVersion = get_new_version(Worker, AuthorID),
                {RelationsChanges, FinalOperations, NewObjects, RemovedObjectsReferences} =
                    commit_operations(Worker, Operations, Version, NewVersion),
                ok = commit_relations_changes(Worker, NewVersion, RelationsChanges),
                ok = validate_no_references_to_entities(
                    Worker, RemovedObjectsReferences, NewVersion
                ),

                %% Publish Kafka event after successful transaction
                ok = publish_commit_event(NewVersion, FinalOperations, AuthorID),
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
        {error, {invalid, _} = Error} ->
            {error, {operation_error, Error}};
        {error, Error} ->
            {error, Error}
    end.

validate_no_references_to_entities(Worker, RemovedObjectsReferences, Version) ->
    %% Ensure there are no inbound references to any removed objects at Version
    %% If any exist, validate_no_references_to_entity/3 will throw
    true = lists:all(
        fun(Ref) ->
            ok =:= validate_no_references_to_entity(Worker, Ref, Version)
        end,
        RemovedObjectsReferences
    ),
    ok.

validate_no_references_to_entity(Worker, Ref, Version) ->
    Ref1 = dmt_mapper:to_string(Ref),
    _ = logger:warning("Validating no references to entity ~p at version ~p", [Ref, Version]),
    case dmt_database:get_referenced_by(Worker, Ref1, Version) of
        [] ->
            ok;
        ReferencedBy when length(ReferencedBy) > 0 ->
            % TODO REPLACE WITH ERROR THAT INDICATES REMOVAL ATTEMPT
            throw({error, {invalid, {objects_not_exist, [{Ref, ReferencedBy}]}}})
    end.

validate_latest_version(Worker, TargetVersion, Ref) ->
    Ref0 = dmt_mapper:to_string(Ref),
    case dmt_database:get_object_latest_version(Worker, Ref0) of
        {ok, MostRecentVersion} when MostRecentVersion > TargetVersion ->
            throw({error, {object_update_too_old, {Ref, MostRecentVersion}}});
        {ok, _MostRecentVersion} ->
            ok;
        {error, not_found} ->
            throw({error, {operation_error, {conflict, {object_not_found, Ref}}}});
        {error, Error} ->
            throw(Error)
    end.

validate_author_exists(Worker, AuthorID) ->
    case dmt_author_database:get(Worker, AuthorID) of
        {ok, _} ->
            ok;
        {error, author_not_found} ->
            throw({error, author_not_found})
    end.

get_new_version(Worker, AuthorID) ->
    case dmt_database:get_new_version(Worker, AuthorID) of
        {ok, NewVersion} ->
            NewVersion;
        {error, Reason} ->
            throw({error, Reason})
    end.

insert_object(Worker, Type, ID0, Version, Data0) ->
    ID1 = dmt_mapper:to_string(ID0),
    Data1 = dmt_mapper:to_string(Data0),
    SearchVector = dmt_mapper:extract_searchable_text_from_term(Data0),

    case dmt_database:insert_object(Worker, ID1, Type, Version, Data1, SearchVector) of
        ok ->
            ID0;
        {error, Reason} ->
            logger:error(
                "insert_object Type: ~p, ID: ~p, Version: ~p, Data: ~p, SearchVector: ~p",
                [Type, ID0, Version, Data1, SearchVector]
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

update_object(Worker, Type, ID0, IsActive, Data0, Version) ->
    Data1 = dmt_mapper:to_string(Data0),
    ID1 = dmt_mapper:to_string(ID0),
    SearchVector = dmt_mapper:extract_searchable_text_from_term(Data0),

    case
        dmt_database:update_object(
            Worker,
            ID1,
            Type,
            Version,
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
    case get_unique_numerical_id(Worker, Type) of
        {ok, NewNumRef} ->
            % TODO need to do it without hardcode
            {Type, NewNumRef};
        {error, sequence_not_enabled} ->
            % Check if type is uuid
            try get_unique_uuid(Worker, Type) of
                {ok, NewUUIDRef} ->
                    {Type, NewUUIDRef}
            catch
                throw:{not_supported, Type} ->
                    throw({error, {object_type_requires_forced_id, Type}})
            end
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

get_unique_numerical_id(Worker, Type) ->
    case dmt_database:get_next_sequence(Worker, Type) of
        {ok, NewID} ->
            NewRef = dmt_object_id:get_numerical_object_id(Type, NewID),
            NewRefString = dmt_mapper:to_string({Type, NewRef}),
            case dmt_database:check_if_object_id_active(Worker, NewRefString) of
                true ->
                    get_unique_numerical_id(Worker, Type);
                false ->
                    {ok, NewRef};
                {error, Reason} ->
                    throw({error, Reason})
            end;
        {error, sequence_not_enabled} ->
            {error, sequence_not_enabled};
        {error, Reason} ->
            throw({error, Reason})
    end.

get_unique_uuid(Worker, Type) ->
    NewUUID = uuid:uuid_to_string(uuid:get_v4_urandom(), binary_standard),
    NewID = dmt_object_id:get_uuid_object_id(Type, NewUUID),
    NewRefString = dmt_mapper:to_string({Type, NewID}),
    case dmt_database:check_if_object_id_active(Worker, NewRefString) of
        true ->
            get_unique_uuid(Worker, Type);
        false ->
            {ok, NewID};
        {error, Reason} ->
            throw({error, Reason})
    end.

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

publish_commit_event(Version, FinalOperations, AuthorID) ->
    try
        %% Get author information
        case dmt_author:get(AuthorID) of
            {ok, Author} ->
                %% Create HistoricalCommit record
                HistoricalCommit = #domain_conf_v2_HistoricalCommit{
                    version = Version,
                    ops = FinalOperations,
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
                        ok;
                    {error, Reason} ->
                        logger:error("Failed to publish commit event for version ~p: ~p", [
                            Version, Reason
                        ]),
                        {error, Reason}
                end;
            {error, Reason} ->
                logger:error("Failed to get author ~p for Kafka event: ~p", [AuthorID, Reason]),
                {error, Reason}
        end
    catch
        Class:Error:Stacktrace ->
            logger:error("Exception in publish_commit_event: ~p:~p~n~p", [Class, Error, Stacktrace]),
            {error, {exception, {Class, Error, Stacktrace}}}
    end.

%% Helper functions for get_related_graph

resolve_version_reference(Worker, undefined) ->
    case dmt_database:get_latest_version(Worker) of
        {ok, LatestVersion} -> {ok, LatestVersion};
        {error, Reason} -> {error, Reason}
    end;
resolve_version_reference(Worker, Version) ->
    case dmt_database:check_version_exists(Worker, Version) of
        true -> {ok, Version};
        false -> {error, version_not_found}
    end.

validate_object_exists(Worker, ObjectRef, Version) ->
    ObjectRefString = dmt_mapper:to_string(ObjectRef),
    case dmt_database:get_object(Worker, ObjectRefString, Version) of
        {ok, _Object} -> ok;
        {error, not_found} -> {error, object_not_found}
    end.
