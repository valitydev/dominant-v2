-module(dmt_database).

-export([get_latest_version/1]).
-export([get_object_latest_version/2]).
-export([get_new_version/2]).
-export([insert_object/6]).
-export([update_object/8]).
-export([get_next_sequence/2]).
-export([check_if_object_id_active/2]).
-export([check_version_exists/2]).
-export([get_object/3]).
-export([get_latest_object/2]).
-export([get_version_creator/2]).
-export([get_all_objects_history/3]).
-export([get_next_history_offset/3]).

get_latest_version(Worker) ->
    Query1 =
        """
        SELECT MAX(version) FROM version;
        """,
    case epg_pool:query(Worker, Query1) of
        {ok, _Columns, [{null}]} ->
            {ok, 0};
        {ok, _Columns, [{Version}]} ->
            {ok, Version};
        {error, Reason} ->
            {error, Reason}
    end.

get_object_latest_version(Worker, ChangedObjectId) ->
    Query0 =
        """
        SELECT version
        FROM entity
        WHERE id = $1
        ORDER BY version DESC
        LIMIT 1
        """,
    case epg_pool:query(Worker, Query0, [ChangedObjectId]) of
        {ok, _Columns, []} ->
            {error, not_found};
        {ok, _Columns, [{MostRecentVersion}]} ->
            {ok, MostRecentVersion};
        {error, Reason} ->
            {error, Reason}
    end.

get_new_version(Worker, AuthorID) ->
    Query1 =
        """
        INSERT INTO version (CREATED_BY)
        VALUES ($1::uuid) RETURNING version;
        """,
    case epg_pool:query(Worker, Query1, [AuthorID]) of
        {ok, 1, _Columns, [{NewVersion}]} ->
            {ok, NewVersion};
        {error, Reason} ->
            {error, Reason}
    end.

insert_object(Worker, ID1, Type, Version, References1, Data1) ->
    Query = """
    INSERT INTO entity
    (id, entity_type, version, references_to, referenced_by, data, is_active)
    VALUES ($1, $2, $3, $4, $5, $6, TRUE);
    """,

    Params = [ID1, Type, Version, References1, [], Data1],

    case epg_pool:query(Worker, Query, Params) of
        {ok, 1} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

update_object(Worker, ID1, Type, Version, References1, ReferencedBy1, Data1, IsActive) ->
    Query = """
    INSERT INTO entity
    (id, entity_type, version, references_to, referenced_by, data, is_active)
    VALUES ($1, $2, $3, $4, $5, $6, $7);
    """,

    Params = [ID1, Type, Version, References1, ReferencedBy1, Data1, IsActive],

    case epg_pool:query(Worker, Query, Params) of
        {ok, 1} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

get_next_sequence(Worker, Type) ->
    Query = """
    UPDATE entity_type
        SET sequence = sequence + 1
        WHERE name = $1 AND has_sequence = true
        RETURNING sequence;
    """,
    case epg_pool:query(Worker, Query, [Type]) of
        {ok, _UpdatedNum, _Columns, [{NextID}]} ->
            {ok, NextID};
        {ok, _UpdatedNum, _Columns, []} ->
            % No rows updated - this means either the type doesn't exist
            % or has_sequence is false
            {error, sequence_not_enabled};
        {error, Reason} ->
            {error, Reason}
    end.

check_if_object_id_active(Worker, ID0) ->
    Query = """
    SELECT is_active
    FROM entity
    WHERE id = $1
    ORDER BY version DESC
    LIMIT 1;
    """,
    case epg_pool:query(Worker, Query, [ID0]) of
        {ok, _Columns, []} ->
            false;
        {ok, _Columns, [{IsActive}]} ->
            IsActive;
        {error, Reason} ->
            {error, Reason}
    end.

check_version_exists(Worker, Version) ->
    Query = """
    SELECT 1
    FROM version
    WHERE version = $1
    LIMIT 1
    """,
    case epg_pool:query(Worker, Query, [Version]) of
        {ok, _Columns, []} ->
            false;
        {ok, _Columns, [_Row]} ->
            true;
        {error, Reason} ->
            {error, Reason}
    end.

get_object(Worker, ID0, Version) ->
    Request = """
    SELECT id,
           entity_type,
           version,
           references_to,
           referenced_by,
           data,
           is_active,
           created_at
    FROM entity
    WHERE id = $1 AND version <= $2
    ORDER BY version DESC
    LIMIT 1
    """,

    case epg_pool:query(Worker, Request, [ID0, Version]) of
        {ok, _Columns, []} ->
            {error, not_found};
        {ok, Columns, Rows} ->
            [Result | _] = dmt_mapper:to_marshalled_maps(Columns, Rows),
            {ok, Result}
    end.

get_latest_object(Worker, ID0) ->
    Request = """
    SELECT id,
           entity_type,
           version,
           references_to,
           referenced_by,
           data,
           is_active,
           created_at
    FROM entity
    WHERE id = $1
    ORDER BY version DESC
    LIMIT 1
    """,

    case epg_pool:query(Worker, Request, [ID0]) of
        {ok, _Columns, []} ->
            {error, not_found};
        {ok, Columns, Rows} ->
            [Result | _] = dmt_mapper:to_marshalled_maps(Columns, Rows),
            {ok, Result}
    end.

get_version_creator(Worker, Version) ->
    Request = """
    SELECT created_by
    FROM version
    WHERE version = $1
    LIMIT 1
    """,

    case epg_pool:query(Worker, Request, [Version]) of
        {ok, _Columns, []} ->
            {error, not_found};
        {ok, _Columns, [{CreatedBy}]} ->
            {ok, CreatedBy}
    end.

get_all_objects_history(Worker, Limit, Offset) ->
    Query = """
    SELECT e.id,
               e.entity_type,
               e.version,
               e.references_to,
               e.referenced_by,
               e.data,
               e.is_active,
               e.created_at
        FROM entity e
        ORDER BY e.version DESC, e.id
        LIMIT $1 OFFSET $2
    """,

    case epg_pool:query(Worker, Query, [Limit, Offset]) of
        {ok, Columns, Rows} ->
            Objects = dmt_mapper:to_marshalled_maps(Columns, Rows),
            {ok, Objects};
        {error, Reason} ->
            {error, Reason}
    end.

get_next_history_offset(Worker, Limit, Offset) ->
    % Сначала проверим, есть ли еще записи после текущего смещения + лимит
    Query = """
    SELECT 1
    FROM entity
    OFFSET $1
    LIMIT 1
    """,

    case epg_pool:query(Worker, Query, [Offset + Limit]) of
        {ok, _, [_]} ->
            % Есть хотя бы одна запись после текущей страницы
            {ok, Offset + Limit};
        {ok, _, []} ->
            % Больше записей нет
            {ok, undefined};
        {error, Reason} ->
            {error, Reason}
    end.
