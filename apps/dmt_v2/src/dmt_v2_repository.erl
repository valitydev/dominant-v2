-module(dmt_v2_repository).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("epgsql/include/epgsql.hrl").

%% API

-export([commit/3]).

%%

commit(Version, Commit, Context) ->
    UpdatedEntities = assemble_operations(Commit, Version),
    case assemble_transaction(Commit, Version) of
        ok ->
            ok
    end.

assemble_operations(Commit, Version) ->
    try
        lists:foldl(
            fun assemble_operations_/2,
            #{
                inserts => [],
                updates => #{},
                version => Version
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
        version := Version
    } = Acc
) ->
    case Operation of
        {insert, #domain_conf_v2_InsertOp{} = InsertOp} ->
            #{
                references := Refers,
                forced_id := ID
            } = NewObject = dmt_v2_object:new_object(InsertOp),

            %%          TODO how to deal with us not knowing new object ID for referenced_by?
            Updates1 = update_objects_added_refs(ID, Refers, UpdatesAcc),

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
                        }
                    }
            end;
        {delete, #domain_conf_v2_RemoveOp{ref = Ref} = RemoveOp} ->
            #{
                references := OriginalReferences
            } = OG = get_original_object_changes(UpdatesAcc, Ref),
            UpdatesAcc1 = update_objects_removed_refs(Ref, OriginalReferences, UpdatesAcc),

            Acc#{
                updates => UpdatesAcc1#{
                    Ref => dmt_v2_object:remove_object(RemoveOp)
                }
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
%%
%%assemble_transaction(Version, Commit) ->
%%    "BEGIN;\n"
%%    "\n"
%%    "DO $$\n"
%%    "DECLARE\n"
%%    "    target_global_version BIGINT := $1;\n"
%%    "    new_entities JSONB := $2;\n"
%%    "    updated_entities JSONB := $3;\n"
%%    "    user_id UUID := $4;\n"
%%    "    new_global_version BIGINT;\n"
%%    "    changed_entities JSONB := '{}';\n"
%%    "    entity_type TEXT;\n"
%%    "    entities JSONB;\n"
%%    "    entity_id JSONB;\n"
%%    "    entity_data JSONB;\n"
%%    "    changed_entity JSONB;\n"
%%    "BEGIN\n"
%%    "    -- Проверка версий обновляемых сущностей\n"
%%    "    FOR entity_type, entities IN SELECT * FROM jsonb_each(updated_entities) LOOP\n"
%%    "        FOR entity_id, entity_data IN SELECT * FROM jsonb_each(entities) LOOP\n"
%%    "            EXECUTE format('\n"
%%    "                SELECT\n"
%%    "                    CASE WHEN version != ($1->''version'')::INT THEN\n"
%%    "                        jsonb_build_object(''id'', id, ''current_version'', version, ''data'', data)\n"
%%    "                    ELSE NULL END\n"
%%    "                FROM %I\n"
%%    "                WHERE id = $2 AND global_version <= $3\n"
%%    "                ORDER BY global_version DESC\n"
%%    "                LIMIT 1\n"
%%    "            ', entity_type)\n"
%%    "            USING entity_data, entity_id, target_global_version\n"
%%    "            INTO changed_entity;\n"
%%    "\n"
%%    "            IF changed_entity IS NOT NULL THEN\n"
%%    "                changed_entities := changed_entities ||\n"
%%    "                    jsonb_build_object(entity_type,\n"
%%    "                        jsonb_build_object(entity_id::text, changed_entity));\n"
%%    "            END IF;\n"
%%    "        END LOOP;\n"
%%    "    END LOOP;\n"
%%    "\n"
%%    "    -- Если есть изменения, прерываем транзакцию\n"
%%    "    IF changed_entities != '{}'::jsonb THEN\n"
%%    "        RAISE EXCEPTION 'Entities have been modified: %', changed_entities;\n"
%%    "    END IF;\n"
%%    "\n"
%%    "    -- Инкрементируем глобальную версию\n"
%%    "    INSERT INTO global_version (created_by)\n"
%%    "    VALUES (user_id)\n"
%%    "    RETURNING version INTO new_global_version;\n"
%%    "\n"
%%    "    -- Добавляем новые сущности\n"
%%    "    FOR entity_type, entities IN SELECT * FROM jsonb_each(new_entities) LOOP\n"
%%    "        FOR entity_id, entity_data IN SELECT * FROM jsonb_each(entities) LOOP\n"
%%    "            EXECUTE format('\n"
%%    "                INSERT INTO %I (id, version, global_version, references_to, referenced_by, data, created_by)\n"
%%    "                VALUES ($1, 1, $2, $3->>''references_to'', $3->>''referenced_by'', $3->>''data'', $4)\n"
%%    "            ', entity_type)\n"
%%    "            USING entity_id, new_global_version, entity_data, user_id;\n"
%%    "        END LOOP;\n"
%%    "    END LOOP;\n"
%%    "\n"
%%    "    -- Обновляем существующие сущности\n"
%%    "    FOR entity_type, entities IN SELECT * FROM jsonb_each(updated_entities) LOOP\n"
%%    "        FOR entity_id, entity_data IN SELECT * FROM jsonb_each(entities) LOOP\n"
%%    "            EXECUTE format('\n"
%%    "                WITH current_entity AS (\n"
%%    "                    SELECT version\n"
%%    "                    FROM %I\n"
%%    "                    WHERE id = $1 AND global_version <= $2\n"
%%    "                    ORDER BY global_version DESC\n"
%%    "                    LIMIT 1\n"
%%    "                )\n"
%%    "                INSERT INTO %I (id, version, global_version, references_to, referenced_by, data, created_by)\n"
%%    "                SELECT $1, current_entity.version + 1, $3,\n"
%%    "                       $4->>''references_to'', $4->>''referenced_by'', $4->>''data'', $5\n"
%%    "                FROM current_entity\n"
%%    "            ', entity_type, entity_type)\n"
%%    "            USING entity_id, target_global_version, new_global_version, entity_data, user_id;\n"
%%    "        END LOOP;\n"
%%    "    END LOOP;\n"
%%    "\n"
%%    "END $$;\n"
%%    "\n"
%%    "COMMIT;".

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
            to_marshalled_maps(Columns, Rows)
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
            to_marshalled_maps(Columns, Rows)
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
