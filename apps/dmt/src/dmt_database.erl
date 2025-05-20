-module(dmt_database).

-export([get_latest_version/1]).
-export([get_object_latest_version/2]).
-export([get_new_version/2]).
-export([insert_object/7]).
-export([update_object/9]).
-export([get_next_sequence/2]).
-export([check_if_object_id_active/2]).
-export([check_version_exists/2]).
-export([get_object/3]).
-export([get_objects/3]).
-export([get_latest_object/2]).
-export([get_version_creator/2]).
-export([get_version/2]).
-export([get_object_history/4]).
-export([get_all_objects_history/3]).
-export([search_objects/6]).
-export([check_entity_type_exists/2]).
-export([get_all_objects/2]).

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

clean_utf8_string(String) ->
    % Convert to binary if it's not already
    Binary =
        case is_binary(String) of
            true -> String;
            false -> unicode:characters_to_binary(String)
        end,
    % Remove any invalid UTF-8 sequences
    case unicode:characters_to_binary(Binary, utf8, utf8) of
        {error, _Invalid, _Rest} ->
            % If there are invalid characters, assume the binary is Latin-1
            % and convert it to UTF-8.
            % binary_to_list(Binary) gives a list of bytes (0-255),
            % which are treated as Latin-1 codepoints.
            unicode:characters_to_binary(binary_to_list(Binary), utf8);
        CleanBinary ->
            CleanBinary
    end.

insert_object(Worker, ID1, Type, Version, References1, Data1, SearchVector) ->
    Query = """
    INSERT INTO entity
    (id, entity_type, version, references_to, referenced_by, data, search_vector, is_active)
    VALUES ($1, $2, $3, $4, $5, $6, to_tsvector('multilingual', $7), TRUE);
    """,

    CleanSearchVector = clean_utf8_string(SearchVector),
    Params = [ID1, Type, Version, References1, [], Data1, CleanSearchVector],

    case epg_pool:query(Worker, Query, Params) of
        {ok, 1} ->
            ok;
        {error, Reason} ->
            logger:error("Error inserting object: ~p~nParams: ~p", [Reason, Params]),
            {error, Reason}
    end.

update_object(
    Worker,
    ID1,
    Type,
    Version,
    References1,
    ReferencedBy1,
    Data1,
    SearchVector,
    IsActive
) ->
    Query = """
    INSERT INTO entity
    (id, entity_type, version, references_to, referenced_by, data, search_vector, is_active)
    VALUES ($1, $2, $3, $4, $5, $6, to_tsvector('multilingual', $7), $8);
    """,

    CleanSearchVector = clean_utf8_string(SearchVector),
    Params = [ID1, Type, Version, References1, ReferencedBy1, Data1, CleanSearchVector, IsActive],

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

    {ok, Columns, Rows} = epg_pool:query(Worker, Request, [ID0, Version]),
    Result0 = dmt_mapper:to_marshalled_maps(Columns, Rows),
    Result1 = dmt_object:filter_out_inactive_objects(Result0),
    case Result1 of
        [] ->
            {error, not_found};
        [Result | _] ->
            {ok, Result}
    end.

get_objects(Worker, IDs, Version) ->
    Request = """
    SELECT DISTINCT ON (id) id,
           entity_type,
           version,
           references_to,
           referenced_by,
           data,
           is_active,
           created_at
    FROM entity
    WHERE id = ANY($1) AND version <= $2
    ORDER BY id, version DESC;
    """,

    case epg_pool:query(Worker, Request, [IDs, Version]) of
        {ok, Columns, Rows} ->
            Results = dmt_mapper:to_marshalled_maps(Columns, Rows),
            {ok, dmt_object:filter_out_inactive_objects(Results)};
        {error, Reason} ->
            logger:error("Error fetching objects: ~p. IDs: ~p, Version: ~p", [Reason, IDs, Version]),
            {error, Reason}
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

    {ok, Columns, Rows} = epg_pool:query(Worker, Request, [ID0]),
    Result0 = dmt_mapper:to_marshalled_maps(Columns, Rows),
    Result1 = dmt_object:filter_out_inactive_objects(Result0),
    case Result1 of
        [] ->
            {error, not_found};
        [Result | _] ->
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

get_version(Worker, Version) ->
    Request = """
    SELECT version, created_at, created_by
    FROM version
    WHERE version = $1
    LIMIT 1
    """,

    case epg_pool:query(Worker, Request, [Version]) of
        {ok, _Columns, []} ->
            {error, not_found};
        {ok, _Columns, [{Version, CreatedAt, CreatedBy}]} ->
            {ok, #{
                version => Version,
                created_at => CreatedAt,
                created_by => CreatedBy
            }}
    end.

get_object_history(Worker, Ref, Limit, Offset) ->
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
        WHERE id = $1
        ORDER BY e.version DESC
        LIMIT $2 OFFSET $3
    """,

    case epg_pool:query(Worker, Query, [Ref, Limit, Offset]) of
        {ok, _Columns, []} ->
            {error, not_found};
        {ok, Columns, Rows} ->
            Objects = dmt_mapper:to_marshalled_maps(Columns, Rows),
            NewOffset0 = Offset + Limit,
            HasMoreResults = has_more_object_history(Worker, Ref, NewOffset0),
            NewOffset1 =
                case HasMoreResults of
                    true -> NewOffset0;
                    false -> undefined
                end,

            {ok, Objects, NewOffset1};
        {error, Reason} ->
            {error, Reason}
    end.

has_more_object_history(Worker, Ref, Offset) ->
    Query = """
    SELECT 1
    FROM entity
    WHERE id = $1
    LIMIT 1
    OFFSET $2
    """,
    case epg_pool:query(Worker, Query, [Ref, Offset]) of
        % At least one more result exists
        {ok, _, [_]} ->
            true;
        % No more results or error
        {ok, _, []} ->
            false;
        {error, Reason} ->
            _ = logger:error("Error checking for more hostory of all objects: ~p", [Reason]),
            false
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
            NewOffset0 = Offset + Limit,
            HasMoreResults = has_more_all_objects_history(Worker, NewOffset0),
            NewOffset1 =
                case HasMoreResults of
                    true -> NewOffset0;
                    false -> undefined
                end,

            {ok, Objects, NewOffset1};
        {error, Reason} ->
            {error, Reason}
    end.

has_more_all_objects_history(Worker, Offset) ->
    % Сначала проверим, есть ли еще записи после текущего смещения + лимит
    Query = """
    SELECT 1
    FROM entity
    LIMIT 1
    OFFSET $1
    """,

    case epg_pool:query(Worker, Query, [Offset]) of
        % At least one more result exists
        {ok, _, [_]} ->
            true;
        % No more results or error
        {ok, _, []} ->
            false;
        {error, Reason} ->
            _ = logger:error("Error checking for more hostory of all objects: ~p", [Reason]),
            false
    end.

search_objects(Worker, <<"*">>, Version, Type, Limit, Offset) ->
    % Use a pattern where the condition is always true when Type is NULL
    TypeValue =
        case Type of
            undefined -> <<"NULL">>;
            _ -> Type
        end,

    Request = """
    SELECT id, entity_type, version, references_to, referenced_by,
    data, is_active, created_at
    FROM entity
    WHERE version <= $1
    AND ($2 = 'NULL' OR entity_type = $2)
    ORDER BY version DESC
    LIMIT $3 OFFSET $4
    """,

    AllParams = [Version, TypeValue, Limit, Offset],

    % Execute the query
    case epg_pool:query(Worker, Request, AllParams) of
        {ok, Columns, Rows} ->
            Objects = dmt_mapper:to_marshalled_maps(Columns, Rows),
            NewOffset0 = Offset + Limit,
            HasMoreResults = has_more_search_results(
                Worker, <<"*">>, Version, TypeValue, NewOffset0
            ),
            NewOffset1 =
                case HasMoreResults of
                    true -> NewOffset0;
                    false -> undefined
                end,

            {ok, {Objects, NewOffset1}};
        {error, Reason} ->
            _ = logger:error("Error fetching search results: ~p", [Reason]),
            _ = logger:error("Error fetching search Params: ~p", [AllParams]),
            {ok, {[], undefined}}
    end;
search_objects(Worker, Query, Version, Type, Limit, Offset) ->
    % Use a pattern where the condition is always true when Type is NULL
    TypeValue =
        case Type of
            undefined -> <<"NULL">>;
            _ -> Type
        end,

    Request = """
    SELECT id, entity_type, version, references_to, referenced_by,
    data, is_active, created_at
    FROM entity
    WHERE search_vector @@ plainto_tsquery('multilingual', $1)
    AND version <= $2
    AND ($3 = 'NULL' OR entity_type = $3)
    ORDER BY version DESC, ts_rank(search_vector, plainto_tsquery('multilingual', $1)) DESC
    LIMIT $4 OFFSET $5
    """,

    AllParams = [Query, Version, TypeValue, Limit, Offset],

    % Execute the query
    case epg_pool:query(Worker, Request, AllParams) of
        {ok, Columns, Rows} ->
            Objects = dmt_mapper:to_marshalled_maps(Columns, Rows),
            NewOffset0 = Offset + Limit,
            HasMoreResults = has_more_search_results(Worker, Query, Version, TypeValue, NewOffset0),
            NewOffset1 =
                case HasMoreResults of
                    true -> NewOffset0;
                    false -> undefined
                end,

            {ok, {Objects, NewOffset1}};
        {error, Reason} ->
            _ = logger:error("Error fetching search results: ~p", [Reason]),
            _ = logger:error("Error fetching search Params: ~p", [AllParams]),
            {ok, {[], undefined}}
    end.

% Helper function to check if there are more search results
has_more_search_results(Worker, <<"*">>, Version, TypeValue, Offset) ->
    CheckMoreQuery = """
    SELECT 1 FROM entity
    WHERE version <= $1
    AND ($2 = 'NULL' OR entity_type = $2)
    LIMIT 1 OFFSET $3
    """,

    case epg_pool:query(Worker, CheckMoreQuery, [Version, TypeValue, Offset]) of
        % At least one more result exists
        {ok, _, [_]} ->
            true;
        % No more results or error
        {ok, _, []} ->
            false;
        {error, Reason} ->
            _ = logger:error("Error checking for more search results: ~p", [Reason]),
            false
    end;
has_more_search_results(Worker, Query, Version, TypeValue, Offset) ->
    CheckMoreQuery = """
    SELECT 1 FROM entity
    WHERE search_vector @@ plainto_tsquery('multilingual', $1)
    AND version <= $2
    AND ($3 = 'NULL' OR entity_type = $3)
    LIMIT 1 OFFSET $4
    """,

    case epg_pool:query(Worker, CheckMoreQuery, [Query, Version, TypeValue, Offset]) of
        % At least one more result exists
        {ok, _, [_]} ->
            true;
        % No more results or error
        {ok, _, []} ->
            false;
        {error, Reason} ->
            _ = logger:error("Error checking for more search results: ~p", [Reason]),
            false
    end.

check_entity_type_exists(Worker, Type) ->
    CheckMoreQuery = """
    SELECT 1 FROM entity_type
    WHERE name = $1
    LIMIT 1
    """,

    case epg_pool:query(Worker, CheckMoreQuery, [Type]) of
        % At least one more result exists
        {ok, _, [_]} ->
            ok;
        % No more results or error
        {ok, _, []} ->
            {error, object_type_not_found};
        {error, Reason} ->
            _ = logger:error("Error checking for more search results: ~p", [Reason]),
            {error, object_type_not_found}
    end.

get_all_objects(Worker, Version) ->
    Query = """
    WITH LatestVersions AS (
        -- Subquery to find the latest version for each entity ID
        SELECT id, MAX(version) AS max_version
        FROM entity
        GROUP BY id
    ),
    LatestActiveStatus AS (
        -- Subquery to get the is_active status of the latest version of each entity
        SELECT e.id, e.is_active
        FROM entity e
        INNER JOIN LatestVersions lv ON e.id = lv.id AND e.version = lv.max_version
    )
    SELECT e.id,
           e.entity_type,
           e.version,
           e.references_to,
           e.referenced_by,
           e.data,
           e.is_active,
           e.created_at
    FROM entity e
    INNER JOIN LatestActiveStatus las ON e.id = las.id
    WHERE e.version <= $1 AND las.is_active = TRUE
    ORDER BY e.id;
    """,
    case epg_pool:query(Worker, Query, [Version]) of
        {ok, Columns, Rows} ->
            Objects = dmt_mapper:to_marshalled_maps(Columns, Rows),
            {ok, Objects};
        {error, Reason} ->
            logger:error("Error fetching all objects for version ~p: ~p", [Version, Reason]),
            {error, Reason}
    end.
