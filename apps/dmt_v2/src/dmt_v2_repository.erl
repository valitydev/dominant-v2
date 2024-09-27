-module(dmt_v2_repository).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("epgsql/include/epgsql.hrl").

%% API

-export([commit/3]).
-export([get_object/2]).
-export([get_local_versions/3]).
-export([get_global_versions/2]).

%%

get_object(ObjectRef, VersionRef) ->
    Version =
        case VersionRef of
            {global_vs, GlobalVS} ->
                GlobalVS;
            {local_vs, _LocalVS} ->
                throw(not_impl)
        end,
    case get_target_object(ObjectRef, Version) of
        {ok, #{
            global_version := GlobalVersion,
            data := Data,
            created_at := CreatedAt
        }} ->
            {ok, #domain_conf_v2_VersionedObject{
                global_version = GlobalVersion,
                %% TODO implement local versions
                local_version = 0,
                object = Data,
                created_at = CreatedAt
            }};
        {error, Reason} ->
            {error, Reason}
    end.

%% Retrieve local versions with pagination
get_local_versions(_Ref, _Limit, _ContinuationToken) ->
    not_impl.

%% Retrieve global versions with pagination
get_global_versions(_Limit, _ContinuationToken) ->
    not_impl.

assemble_operations(Commit) ->
    try
        lists:foldl(
            fun assemble_operations_/2,
            #{
                inserts => [],
                updates => #{},
                objects_being_updated => []
            },
            Commit#domain_conf_v2_Commit.ops
        )
    catch
        {error, Error} ->
            {error, Error}
    end.

assemble_operations_(
    Operation,
    #{
        inserts := InsertsAcc,
        updates := UpdatesAcc,
        objects_being_updated := UpdatedObjectsAcc
    } = Acc
) ->
    case Operation of
        {insert, #domain_conf_v2_InsertOp{} = InsertOp} ->
            {ok, NewObject} = dmt_v2_object:new_object(InsertOp),
            #{
                tmp_id := TmpID,
                references := Refers
            } = NewObject,

            Updates1 = update_objects_added_refs({temporary, TmpID}, Refers, UpdatesAcc),

            Acc#{
                inserts => [NewObject | InsertsAcc],
                updates => Updates1
            };
        {update, #domain_conf_v2_UpdateOp{targeted_ref = Ref} = UpdateOp} ->
            ExistingUpdates = maps:get(Ref, UpdatesAcc, #{}),
            case get_original_object_changes(UpdatesAcc, Ref) of
                #{data := _} ->
                    throw({error, {double_update_not_allowed, UpdateOp}});
                Changes ->
                    ObjectUpdate = dmt_v2_object:update_object(UpdateOp, ExistingUpdates),
                    UpdatesAcc1 = update_referenced_objects(Changes, ObjectUpdate, UpdatesAcc),
                    Acc#{
                        updates => UpdatesAcc1#{
                            Ref => ObjectUpdate
                        },
                        objects_being_updated => [Ref | UpdatedObjectsAcc]
                    }
            end;
        {delete, #domain_conf_v2_RemoveOp{ref = Ref} = RemoveOp} ->
            #{
                references := OriginalReferences
            } = get_original_object_changes(UpdatesAcc, Ref),
            UpdatesAcc1 = update_objects_removed_refs(Ref, OriginalReferences, UpdatesAcc),

            Acc#{
                updates => UpdatesAcc1#{
                    Ref => dmt_v2_object:remove_object(RemoveOp)
                },
                objects_being_updated => [Ref | UpdatedObjectsAcc]
            }
    end.

update_referenced_objects(OriginalObjectChanges, ObjectChanges, Updates) ->
    #{
        id := ObjectID,
        references := OriginalReferences
    } = OriginalObjectChanges,
    #{references := NewVersionReferences} = ObjectChanges,
    ORS = ordsets:from_list(OriginalReferences),
    NVRS = ordsets:from_list(NewVersionReferences),
    AddedRefs = ordsets:subtract(NVRS, ORS),
    RemovedRefs = ordsets:subtract(ORS, NVRS),

    Updates1 = update_objects_added_refs(ObjectID, AddedRefs, Updates),
    update_objects_removed_refs(ObjectID, RemovedRefs, Updates1).

update_objects_added_refs(ObjectID, AddedRefs, Updates) ->
    lists:foldl(
        fun(Ref, Acc) ->
            #{
                id := UpdatedObjectID,
                referenced_by := RefdBy0
            } = OG = get_original_object_changes(Acc, Ref),

            Acc#{
                UpdatedObjectID =>
                    OG#{
                        referenced_by => [ObjectID | RefdBy0]
                    }
            }
        end,
        Updates,
        AddedRefs
    ).

update_objects_removed_refs(ObjectID, RemovedRefs, Updates) ->
    lists:foldl(
        fun(Ref, Acc) ->
            #{
                id := UpdatedObjectID,
                referenced_by := RefdBy0
            } = OG = get_original_object_changes(Acc, Ref),
            RefdBy1 = ordsets:from_list(RefdBy0),
            RefdBy2 = ordsets:del_element(ObjectID, RefdBy1),
            Acc#{
                UpdatedObjectID =>
                    OG#{
                        referenced_by => ordsets:to_list(RefdBy2)
                    }
            }
        end,
        Updates,
        RemovedRefs
    ).

get_original_object_changes(Updates, Ref) ->
    case Updates of
        #{Ref := Object} ->
            Object;
        _ ->
            #{
                id := ID,
                type := Type,
                referenced_by := RefdBy,
                references := Refers
            } = get_latest_target_object(Ref),
            %%          NOTE this is done in order to decouple object type from object change type
            #{
                id => ID,
                type => Type,
                referenced_by => RefdBy,
                references => Refers
            }
    end.

%% NOTE Add new tables here
-define(TABLES, [
    category,
    currency,
    business_schedule,
    calendar,
    payment_method,
    payout_method,
    bank,
    contract_template,
    term_set_hierarchy,
    payment_institution,
    provider,
    terminal,
    inspector,
    system_account_set,
    external_account_set,
    proxy,
    globals,
    cash_register_provider,
    routing_rules,
    bank_card_category,
    criterion,
    document_type,
    payment_service,
    payment_system,
    bank_card_token_service,
    mobile_op_user,
    crypto_currency,
    country,
    trade_bloc,
    identity_provider,
    limit_config
]).

commit(Version, Commit, CreatedBy) ->
    Changes = assemble_operations(Commit),
    {InsertObjects, UpdateObjects, ChangedObjectIds} = prepare_changes(Changes),
    case
        epgsql_pool:transaction(default_pool,
            fun(Worker) ->
                ok = check_versions_sql(Worker, ChangedObjectIds, Version),
                NewVersion = get_new_version(Worker, CreatedBy),
                PermanentIDsMaps = insert_objects(Worker, InsertObjects, NewVersion),
                ok = update_objects(Worker, UpdateObjects, NewVersion)
            end)
    of
        {ok, _, [{NewGlobalVersion}]} ->
            {ok, NewGlobalVersion};
        {error, {error, error, _, conflict_detected, Msg, _}} ->
            {error, {conflict, Msg}};
        Error ->
            {error, Error}
    end.

%%build_commit_query(Tables) ->
%%    CheckVersionsSql = build_check_versions_sql(Tables),
%%    InsertObjectsSql = build_insert_objects_sql(Tables),
%%    UpdateObjectsSql = build_update_objects_sql(Tables),
%%
%%    io_lib:format("""
%%    DO $$
%%    DECLARE
%%        new_global_version bigint;
%%        conflicting_objects jsonb := '[]'::jsonb;
%%        temp_id_mapping jsonb := '{}'::jsonb;
%%    BEGIN
%%        -- Шаг 1: Проверка версий и поиск конфликтов
%%
%%        ~p
%%
%%        IF jsonb_array_length(conflicting_objects) > 0 THEN
%%            RAISE EXCEPTION 'Conflict detected for objects: %', conflicting_objects;
%%        END IF;
%%
%%        -- Шаг 2: Инкремент глобальной версии
%%        INSERT INTO global_version (created_by) VALUES ($3::uuid) RETURNING version INTO new_global_version;
%%
%%        -- Шаг 3: Добавление новых объектов
%%        WITH new_objects AS (
%%            ~p
%%        )
%%        SELECT jsonb_object_agg(tmp_id, id) INTO temp_id_mapping FROM new_objects;
%%
%%        -- Шаг 4: Обновление существующих объектов
%%
%%        ~p
%%
%%        -- Возвращаем новую глобальную версию
%%        PERFORM pg_advisory_xact_lock(cast((-1) as int), cast(new_global_version as int));
%%        END $$;
%%    """, [CheckVersionsSql, InsertObjectsSql, UpdateObjectsSql]).


check_versions_sql(Worker, ChangedObjectIds, Version) ->
    lists:foreach(
        fun ({ChangedObjectType, ChangedObjectRef} = ChangedObjectId) ->
            Query0 =
                """
                SELECT id, global_version
                FROM $1 WHERE id = ANY($1::jsonb[])
                ORDER BY global_version DESC
                LIMIT 1
                """,
            case epgsql_pool:query(Worker, Query0, [ChangedObjectType, ChangedObjectRef]) of
                {ok, _Columns, []} ->
                    throw({unknown_object_update, ChangedObjectId});
                {ok, _Columns, [{ChangedObjectRef, MostRecentVersion}]} when MostRecentVersion > Version ->
                    throw({object_update_too_old, {ChangedObjectRef, MostRecentVersion}});
                {ok, _Columns, [{ChangedObjectRef, MostRecentVersion}]} ->
                    ok;
                {error, Reason} ->
                    throw({error, Reason})
            end
        end,
        ChangedObjectIds
    ),
    ok.

get_new_version(Worker, CreatedBy) ->
    Query1 =
        """
        INSERT INTO GLOBAL_VERSION (CREATED_BY)
        VALUES ($1::uuid) RETURNING version;
        """,
    case epgsql_pool:query(Worker, Query1, [CreatedBy]) of
        {ok, 1, _Columns, [{NewVersion}]} ->
            NewVersion;
        {error, Reason} ->
            throw({error, Reason})
    end.

%%get_new_version(Version) ->
%%    Query0 =
%%        """
%%                SELECT id, global_version
%%                FROM $1 WHERE id = ANY($1::jsonb[])
%%                ORDER BY global_version DESC
%%                LIMIT 1
%%                """,
%%    case epgsql_pool:query(Worker, Query0, [ChangedObjectType, ChangedObjectRef]) of
%%        {ok, _Columns, []} ->
%%            throw({unknown_object_update, ChangedObjectId});
%%        {ok, _Columns, [{ChangedObjectRef, MostRecentVersion}]} when MostRecentVersion > Version ->
%%            throw({object_update_too_old, {ChangedObjectRef, MostRecentVersion}});
%%        {ok, _Columns, [{ChangedObjectRef, MostRecentVersion}]} ->
%%            ok;
%%        {error, Reason} ->
%%            throw({error, Reason})
%%    end.

insert_objects(Worker, InsertObjects, Version) ->
    lists:foldl(
        fun(InsertObject, Acc) ->
            #{
                tmp_id := TmpID,
                type := Type,
                forced_id := ForcedID,
                references := References,
                data := Data
            } = InsertObject,
            {ID, Sequence} = get_insert_object_id(Worker, ForcedID, Type),
            ID = insert_object(Worker, Type, ID, Sequence, Version, References, Data),
            Acc#{TmpID => ID}
        end,
        #{},
        InsertObjects
    ).

insert_object(Worker, Type, ID, Sequence, Version, References, Data) ->
    {Query, Params} = case check_if_force_id_required(Worker, Type) of
        true ->
            Query0 =
                """
            INSERT INTO $1 (id, global_version, references_to, data)
                VALUES ($2, $3, $4, $5)
            """,
            Params0 = [Type, ID, Version, References, Data],
            {Query0, Params0};
        false ->
            Query1 =
                """
            INSERT INTO $1 (id, sequence, global_version, references_to, data)
                VALUES ($2, $3, $4, $5, $6)
            """,
            Params1 = [Type, ID, Sequence, Version, References, Data],
            {Query1, Params1}
    end,
    case epgsql_pool:query(Worker, Query, Params) of
        {ok, _Columns, _Rows} ->
            ID;
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
get_insert_object_id(Worker, ForcedID, Type) ->
    case check_if_id_exists(Worker, ForcedID, Type) of
        true ->
            throw({error, {forced_id_exists, ForcedID}});
        false ->
            {ForcedID, null}
    end.


check_if_force_id_required(Worker, Type) ->
    Query = """
    SELECT column_name
        FROM information_schema.columns
        WHERE table_name = $1 AND column_name = 'sequence';
    """,
    case epgsql_pool:query(Worker, Query, [Type]) of
        {ok, _Columns, []} ->
            true;
        {ok, _Columns, [{"sequence"}]} ->
            false;
        {error, Reason} ->
            throw({error, Reason})
    end.

get_last_sequence(Worker, Type) ->
    Query = """
    SELECT MAX(sequence)
    FROM $1;
    """,
    case epgsql_pool:query(Worker, Query, [Type]) of
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
            NewID = dmt_v2_object_id:get_object_id(Type, NextSequence),
            case check_if_id_exists(Worker, ID, Type) of
                false ->
                    {halt, {NewID, NextSequence}};
                true ->
                    {cont, {ID, NextSequence}}
            end
        end,
        {undefined, LastSequenceInType},
        lists:seq(1, 100)
    ).

check_if_id_exists(Worker, ID, Type) ->
    Query = """
            SELECT id
            FROM $1
            WHERE id = $2::text
            """,
    case epgsql_pool:query(Worker, Query, [Type, ID]) of
        {ok, _Columns, []} ->
            false;
        {ok, _Columns, [{ID}]} ->
            true;
        {error, Reason} ->
            throw({error, Reason})
    end.


build_insert_objects_sql(Tables) ->
    InsertClauses = lists:map(
        fun(Table) ->
            TableStr = atom_to_list(Table),
            Res = io_lib:format(
                """
                INSERT INTO ~p (id, global_version, references_to, referenced_by, data, created_by)
                SELECT
                    COALESCE(id_generator, id),
                    new_global_version,
                    references_to,
                    referenced_by,
                    data,
                    $3::uuid
                FROM jsonb_to_recordset($4::jsonb) AS x(
                    tmp_id text,
                    id jsonb,
                    id_generator text,
                    type text,
                    references_to jsonb[],
                    referenced_by jsonb[],
                    data jsonb
                )
                WHERE type = '~p'
                RETURNING tmp_id, id
                """,
                [TableStr, TableStr]
            ),
            io:format("~p", [Res]),
            Res
        end,
        Tables
    ),
    lists:join(InsertClauses, " UNION ALL ").

build_update_objects_sql(Tables) ->
    UpdateClauses = lists:map(
        fun(Table) ->
            TableStr = atom_to_list(Table),
            Res = io_lib:format(
                """
                UPDATE ~p
                SET
                    global_version = new_global_version,
                    references_to = x.references_to,
                    referenced_by = (
                        SELECT jsonb_agg(
                            CASE
                                WHEN elem::text LIKE 'temporary:%'
                                THEN temp_id_mapping->substring(elem::text FROM 'temporary:(.*)')
                                ELSE elem
                            END
                        )
                        FROM jsonb_array_elements(x.referenced_by) elem
                    ),
                    data = x.data
                FROM jsonb_to_recordset($5::jsonb) AS x(
                    id jsonb,
                    type text,
                    references_to jsonb[],
                    referenced_by jsonb[],
                    data jsonb
                )
                WHERE ~p.id = x.id AND x.type = '~p'
                """,
                [TableStr, TableStr, TableStr]
            ),
            io:format("~p", [Res]),
            Res
        end,
        Tables
    ),
    io_lib:format(
        """
        WITH updates AS (~p)
        SELECT 1;
        """,
        [lists:join(UpdateClauses, " UNION ALL ")]
    ).

get_target_object(Ref, Version) ->
    {Type, _} = Ref,
    Request = """
    SELECT tt.id,
           tt.global_version,
           tt.references_to,
           tt.referenced_by,
           tt.data,
           tt.created_at,
           tt.created_by
    FROM $1 as tt
    ORDER BY global_version DESC
    WHERE pm.id = $2::jsonb AND pm.global_version <= $3
    LIMIT 1
    """,
    Result = epgsql_pool:query(default_pool, Request, [Type, Ref, Version]),
    case Result of
        {ok, _Columns, []} ->
            {error, {object_not_found, Ref, Version}};
        {ok, Columns, Rows} ->
            [Result | _] = to_marshalled_maps(Columns, Rows),
            {ok, Result}
    end.

get_latest_target_object(Ref) ->
    {Type, _} = Ref,
    Request = """
    SELECT tt.id,
           tt.global_version,
           tt.references_to,
           tt.referenced_by,
           tt.data,
           tt.created_at,
           tt.created_by
    FROM $1 as tt
    ORDER BY global_version DESC
    WHERE pm.id = $2::jsonb
    LIMIT 1
    """,
    Result = epgsql_pool:query(default_pool, Request, [Type, Ref]),
    case Result of
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
    <<"global_version">> := Version,
    <<"references_to">> := ReferencesTo,
    <<"referenced_by">> := ReferencedBy,
    <<"data">> := Data,
    <<"created_at">> := CreatedAt,
    <<"created_by">> := CreatedBy
}) ->
    dmt_v2_object:just_object(
        ID,
        Version,
        ReferencesTo,
        ReferencedBy,
        Data,
        CreatedAt,
        CreatedBy
    ).

prepare_changes(#{
    inserts := InsertsAcc,
    updates := UpdatesAcc,
    objects_being_updated := UpdatedObjectsAcc
}) ->
    io:format("InsertsAcc: ~p~n", [InsertsAcc]),
    io:format("UpdatesAcc: ~p~n", [UpdatesAcc]),
    io:format("UpdatedObjectsAcc: ~p~n", [UpdatedObjectsAcc]),
    {InsertsAcc, UpdatesAcc, UpdatedObjectsAcc}.
