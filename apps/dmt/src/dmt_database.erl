-module(dmt_database).

-export([get_latest_version/1]).
-export([get_object_latest_version/2]).
-export([get_new_version/2]).
-export([insert_object/6]).
-export([update_object/7]).
-export([insert_relations/5]).
-export([get_referenced_by/3]).
-export([get_references_to/3]).
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
-export([get_related_graph/7]).
-export([parse_entity_validation_error/1]).

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
    {ok, #{
        git_ref := GitRef
    }} = application:get_env(dmt, damsel_version_info),
    Query1 =
        """
        INSERT INTO version (CREATED_BY, protocol_version)
        VALUES ($1::uuid, $2) RETURNING version;
        """,
    case epg_pool:query(Worker, Query1, [AuthorID, GitRef]) of
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

insert_object(Worker, ID1, Type, Version, Data1, SearchVector) ->
    Query = """
    INSERT INTO entity
    (id, entity_type, version, data, search_vector, is_active)
    VALUES ($1, $2, $3, $4, to_tsvector('multilingual', $5), TRUE);
    """,

    CleanSearchVector = clean_utf8_string(SearchVector),
    Params = [ID1, Type, Version, Data1, CleanSearchVector],

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
    Data1,
    SearchVector,
    IsActive
) ->
    Query = """
    INSERT INTO entity
    (id, entity_type, version, data, search_vector, is_active)
    VALUES ($1, $2, $3, $4, to_tsvector('multilingual', $5), $6);
    """,

    CleanSearchVector = clean_utf8_string(SearchVector),
    Params = [ID1, Type, Version, Data1, CleanSearchVector, IsActive],

    case epg_pool:query(Worker, Query, Params) of
        {ok, 1} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

insert_relations(Worker, SourceID, TargetID, Version, IsActive) ->
    Query = """
    INSERT INTO entity_relation
    (source_entity_id, target_entity_id, version, is_active)
    VALUES ($1, $2, $3, $4)
    """,
    Params = [SourceID, TargetID, Version, IsActive],

    case epg_pool:query(Worker, Query, Params) of
        {ok, 1} ->
            ok;
        {error, {error, error, <<"23505">>, _, Message, _}} ->
            % Handle unique constraint violations
            logger:error("Unique constraint violation inserting entity relations: ~p~nParams: ~p", [Message, Params]),
            {error, {duplicate_relation, Message}};
        {error, {error, error, _, _, Message, _}} when is_binary(Message) ->
            case parse_entity_validation_error(Message) of
                {source_entity_not_found, EntityId} ->
                    logger:error("Source entity ~p does not exist when inserting relation: ~p", [EntityId, Params]),
                    {error, {source_entity_not_found, EntityId}};
                {target_entity_not_found, EntityId} ->
                    logger:error("Target entity ~p does not exist when inserting relation: ~p", [EntityId, Params]),
                    {error, {target_entity_not_found, EntityId}};
                undefined ->
                    logger:error("Error inserting entity relations: ~p~nParams: ~p", [Message, Params]),
                    {error, Message}
            end;
        {error, Reason} ->
            logger:error("Error inserting entity relations: ~p~nParams: ~p", [Reason, Params]),
            {error, Reason}
    end.

%% @doc Parse validation error messages from the validate_entity_exists trigger
%% Expected format: "ENTITY_NOT_EXISTS|SOURCE|entity_id" or "ENTITY_NOT_EXISTS|TARGET|entity_id"
-spec parse_entity_validation_error(binary()) ->
    {source_entity_not_found, binary()}
    | {target_entity_not_found, binary()}
    | unknown.
parse_entity_validation_error(Message) ->
    case binary:split(Message, <<"|">>, [global]) of
        [<<"ENTITY_NOT_EXISTS">>, <<"SOURCE">>, EntityId] ->
            {source_entity_not_found, dmt_mapper:from_string(EntityId)};
        [<<"ENTITY_NOT_EXISTS">>, <<"TARGET">>, EntityId] ->
            {target_entity_not_found, dmt_mapper:from_string(EntityId)};
        _ ->
            undefined
    end.

get_references_to(Worker, ID, Version) ->
    Query = """
    WITH LatestVersionAtRequestedTime AS (
        -- Find the latest version for each entity relation at or before the requested version
        SELECT source_entity_id, MAX(version) AS max_version_at_time
        FROM entity_relation
        WHERE version <= $1
        GROUP BY source_entity_id
    ),
    ActiveStatusAtRequestedTime AS (
        -- Get the is_active status at the requested time
        SELECT e.source_entity_id, e.is_active
        FROM entity_relation e
        INNER JOIN LatestVersionAtRequestedTime lv
        ON e.source_entity_id = lv.source_entity_id AND e.version = lv.max_version_at_time
    )
    SELECT DISTINCT target_entity_id
    FROM entity_relation e
    INNER JOIN ActiveStatusAtRequestedTime las ON e.source_entity_id = las.source_entity_id
    WHERE e.source_entity_id = $2
    AND e.version <= $1
    AND las.is_active = TRUE
    """,

    Params = [Version, ID],
    case epg_pool:query(Worker, Query, Params) of
        {ok, _Columns, Refs} ->
            lists:map(fun({Res}) -> dmt_mapper:from_string(Res) end, Refs);
        {error, Reason} ->
            {error, Reason}
    end.

get_referenced_by(Worker, ID, Version) ->
    Query = """
    WITH LatestVersionAtRequestedTime AS (
        -- Find the latest version for each entity relation at or before the requested version
        SELECT target_entity_id, MAX(version) AS max_version_at_time
        FROM entity_relation
        WHERE version <= $1
        GROUP BY target_entity_id
    ),
    ActiveStatusAtRequestedTime AS (
        -- Get the is_active status at the requested time
        SELECT e.target_entity_id, e.is_active
        FROM entity_relation e
        INNER JOIN LatestVersionAtRequestedTime lv
        ON e.target_entity_id = lv.target_entity_id AND e.version = lv.max_version_at_time
    )
    SELECT DISTINCT source_entity_id
    FROM entity_relation e
    INNER JOIN ActiveStatusAtRequestedTime las ON e.target_entity_id = las.target_entity_id
    WHERE e.target_entity_id = $2
    AND e.version <= $1
    AND las.is_active = TRUE
    """,

    Params = [Version, ID],
    case epg_pool:query(Worker, Query, Params) of
        {ok, _Columns, Refs} ->
            lists:map(fun({Res}) -> dmt_mapper:from_string(Res) end, Refs);
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
    WITH LatestVersionAtRequestedTime AS (
        -- Find the latest version for this entity at or before the requested version
        SELECT id, MAX(version) AS max_version_at_time
        FROM entity
        WHERE id = $1 AND version <= $2
        GROUP BY id
    ),
    ActiveStatusAtRequestedTime AS (
        -- Get the is_active status at the requested time
        SELECT e.id, e.is_active
        FROM entity e
        INNER JOIN LatestVersionAtRequestedTime lv ON e.id = lv.id AND e.version = lv.max_version_at_time
    )
    SELECT e.id,
           e.entity_type,
           e.version,
           e.data,
           e.is_active,
           e.created_at
    FROM entity e
    INNER JOIN ActiveStatusAtRequestedTime las ON e.id = las.id
    WHERE e.id = $1 AND e.version <= $2 AND las.is_active = TRUE
    ORDER BY e.version DESC
    LIMIT 1
    """,

    case epg_pool:query(Worker, Request, [ID0, Version]) of
        {ok, _Columns, []} ->
            {error, not_found};
        {ok, Columns, Rows} ->
            Result = dmt_mapper:to_marshalled_maps(Columns, Rows),
            case Result of
                [] ->
                    {error, not_found};
                [ResultObj | _] ->
                    {ok, ResultObj}
            end
    end.

get_objects(Worker, IDs, Version) ->
    Request = """
    WITH LatestVersionAtRequestedTime AS (
        -- Find the latest version for each entity at or before the requested version
        SELECT id, MAX(version) AS max_version_at_time
        FROM entity
        WHERE id = ANY($1) AND version <= $2
        GROUP BY id
    ),
    ActiveStatusAtRequestedTime AS (
        -- Get the is_active status at the requested time
        SELECT e.id, e.is_active
        FROM entity e
        INNER JOIN LatestVersionAtRequestedTime lv ON e.id = lv.id AND e.version = lv.max_version_at_time
    )
    SELECT DISTINCT ON (e.id) e.id,
           e.entity_type,
           e.version,
           e.data,
           e.is_active,
           e.created_at
    FROM entity e
    INNER JOIN ActiveStatusAtRequestedTime las ON e.id = las.id
    WHERE e.id = ANY($1) AND e.version <= $2 AND las.is_active = TRUE
    ORDER BY e.id, e.version DESC
    """,

    case epg_pool:query(Worker, Request, [IDs, Version]) of
        {ok, Columns, Rows} ->
            Results = dmt_mapper:to_marshalled_maps(Columns, Rows),
            {ok, Results};
        {error, Reason} ->
            logger:error("Error fetching objects: ~p. IDs: ~p, Version: ~p", [Reason, IDs, Version]),
            {error, Reason}
    end.

get_latest_object(Worker, ID0) ->
    Request = """
    WITH LatestVersion AS (
        -- Find the latest version for this entity
        SELECT id, MAX(version) AS max_version
        FROM entity
        WHERE id = $1
        GROUP BY id
    )
    SELECT e.id,
           e.entity_type,
           e.version,
           e.data,
           e.is_active,
           e.created_at
    FROM entity e
    INNER JOIN LatestVersion lv ON e.id = lv.id AND e.version = lv.max_version
    WHERE e.id = $1 AND e.is_active = TRUE
    ORDER BY e.version DESC
    LIMIT 1
    """,

    case epg_pool:query(Worker, Request, [ID0]) of
        {ok, _Columns, []} ->
            {error, not_found};
        {ok, Columns, Rows} ->
            Result = dmt_mapper:to_marshalled_maps(Columns, Rows),
            case Result of
                [] ->
                    {error, not_found};
                [ResultObj | _] ->
                    {ok, ResultObj}
            end
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
    WITH LatestVersionAtRequestedTime AS (
        -- Find the latest version for each entity at or before the requested version
        SELECT id, MAX(version) AS max_version_at_time
        FROM entity
        WHERE version <= $1
        GROUP BY id
    ),
    ActiveStatusAtRequestedTime AS (
        -- Get the is_active status at the requested time
        SELECT e.id, e.is_active
        FROM entity e
        INNER JOIN LatestVersionAtRequestedTime lv ON e.id = lv.id AND e.version = lv.max_version_at_time
    )
    SELECT DISTINCT ON (e.id) e.id,
           e.entity_type,
           e.version,
           e.data,
           e.is_active,
           e.created_at
    FROM entity e
    INNER JOIN ActiveStatusAtRequestedTime las ON e.id = las.id
    WHERE e.version <= $1
    AND ($2 = 'NULL' OR e.entity_type = $2)
    AND las.is_active = TRUE
    ORDER BY e.id, e.version DESC
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
    WITH LatestVersionAtRequestedTime AS (
        -- Find the latest version for each entity at or before the requested version
        SELECT id, MAX(version) AS max_version_at_time
        FROM entity
        WHERE version <= $2
        GROUP BY id
    ),
    ActiveStatusAtRequestedTime AS (
        -- Get the is_active status at the requested time
        SELECT e.id, e.is_active
        FROM entity e
        INNER JOIN LatestVersionAtRequestedTime lv ON e.id = lv.id AND e.version = lv.max_version_at_time
    )
    SELECT DISTINCT ON (e.id) e.id,
           e.entity_type,
           e.version,
           e.data,
           e.is_active,
           e.created_at
    FROM entity e
    INNER JOIN ActiveStatusAtRequestedTime las ON e.id = las.id
    WHERE e.search_vector @@ plainto_tsquery('multilingual', $1)
    AND e.version <= $2
    AND ($3 = 'NULL' OR e.entity_type = $3)
    AND las.is_active = TRUE
    ORDER BY e.id, e.version DESC, ts_rank(e.search_vector, plainto_tsquery('multilingual', $1)) DESC
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
    WITH LatestVersionAtRequestedTime AS (
        -- Find the latest version for each entity at or before the requested version
        SELECT id, MAX(version) AS max_version_at_time
        FROM entity
        WHERE version <= $1
        GROUP BY id
    ),
    ActiveStatusAtRequestedTime AS (
        -- Get the is_active status at the requested time
        SELECT e.id, e.is_active
        FROM entity e
        INNER JOIN LatestVersionAtRequestedTime lv ON e.id = lv.id AND e.version = lv.max_version_at_time
    )
    SELECT 1 FROM entity e
    INNER JOIN ActiveStatusAtRequestedTime las ON e.id = las.id
    WHERE e.version <= $1
    AND ($2 = 'NULL' OR e.entity_type = $2)
    AND las.is_active = TRUE
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
    WITH LatestVersionAtRequestedTime AS (
        -- Find the latest version for each entity at or before the requested version
        SELECT id, MAX(version) AS max_version_at_time
        FROM entity
        WHERE version <= $2
        GROUP BY id
    ),
    ActiveStatusAtRequestedTime AS (
        -- Get the is_active status at the requested time
        SELECT e.id, e.is_active
        FROM entity e
        INNER JOIN LatestVersionAtRequestedTime lv ON e.id = lv.id AND e.version = lv.max_version_at_time
    )
    SELECT 1 FROM entity e
    INNER JOIN ActiveStatusAtRequestedTime las ON e.id = las.id
    WHERE e.search_vector @@ plainto_tsquery('multilingual', $1)
    AND e.version <= $2
    AND ($3 = 'NULL' OR e.entity_type = $3)
    AND las.is_active = TRUE
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

get_related_graph(
    Worker, ObjectRef, Version, Depth, IncludeInbound, IncludeOutbound, TypeFilter
) ->
    case get_related_graph_edges(Worker, ObjectRef, Version, Depth, IncludeInbound, IncludeOutbound) of
        {ok, {EntityIds, Edges}} ->
            logger:error("EntityIds: ~p, Edges: ~p", [EntityIds, Edges]),
            get_objects_and_filter(Worker, EntityIds, Version, TypeFilter, Edges, ObjectRef);
        {error, Reason} ->
            logger:error("Error in graph edge traversal for ~p: ~p", [ObjectRef, Reason]),
            {error, Reason}
    end.

%% Helper function to get objects and apply type filter
get_objects_and_filter(Worker, EntityIds, Version, TypeFilter, Edges, ObjectRef) ->
    case get_objects(Worker, EntityIds, Version) of
        {ok, AllNodes} ->
            FilteredNodes = filter_nodes_by_type(AllNodes, TypeFilter),
            FilteredEdges = filter_edges_by_nodes(Edges, FilteredNodes),
            logger:error("FilteredNodes: ~p, FilteredEdges: ~p", [FilteredNodes, FilteredEdges]),
            {ok, {FilteredNodes, FilteredEdges}};
        {error, Reason} ->
            logger:error("Error fetching objects for graph ~p: ~p", [ObjectRef, Reason]),
            {error, Reason}
    end.

%% Filter nodes by type if type filter is specified
filter_nodes_by_type(Nodes, undefined) ->
    Nodes;
filter_nodes_by_type(Nodes, FilterType) ->
    FilterTypeBinary = atom_to_binary(FilterType, utf8),
    [Node || Node <- Nodes, maps:get(type, Node) =:= FilterTypeBinary].

filter_edges_by_nodes(Edges, Nodes) ->
    logger:error("filter_edges_by_nodes Edges: ~p, Nodes: ~p", [Edges, Nodes]),
    lists:filter(
        fun(#{source_ref := SourceRef, target_ref := TargetRef}) ->
            logger:error("Filter SourceRef: ~p, TargetRef: ~p", [SourceRef, TargetRef]),
            lists:any(
                fun(#{id := NodeId}) ->
                    NodeId =:= SourceRef
                end,
                Nodes
            ) andalso
                lists:any(
                    fun(#{id := NodeId}) ->
                        NodeId =:= TargetRef
                    end,
                    Nodes
                )
        end,
        Edges
    ).

get_related_graph_edges(Worker, ObjectRef, Version, Depth, IncludeInbound, IncludeOutbound) ->
    Query = """
    WITH RECURSIVE
    -- Find active relationships at the requested version
    ActiveRelations AS (
        SELECT DISTINCT
            r1.source_entity_id,
            r1.target_entity_id
        FROM entity_relation r1
        WHERE r1.is_active = TRUE
        AND r1.version = (
            SELECT MAX(r2.version)
            FROM entity_relation r2
            WHERE r2.source_entity_id = r1.source_entity_id
            AND r2.target_entity_id = r1.target_entity_id
            AND r2.version <= $2
        )
    ),
    -- Recursive graph traversal - collect entity IDs only
    GraphTraversal AS (
        -- Base case: start with the root object
        SELECT
            $1::TEXT as entity_id,
            0 as depth,
            ARRAY[$1::TEXT] as path

        UNION ALL

        -- Recursive case: find connected entities
        SELECT DISTINCT
            CASE
                WHEN $4 = TRUE AND $5 = TRUE THEN
                    -- Include both inbound and outbound
                    CASE
                        WHEN ar.source_entity_id = gt.entity_id THEN ar.target_entity_id
                        WHEN ar.target_entity_id = gt.entity_id THEN ar.source_entity_id
                        ELSE NULL
                    END
                WHEN $4 = TRUE AND $5 = FALSE THEN
                    -- Include only inbound (entities that reference current entity)
                    CASE WHEN ar.target_entity_id = gt.entity_id THEN ar.source_entity_id ELSE NULL END
                WHEN $4 = FALSE AND $5 = TRUE THEN
                    -- Include only outbound (entities that current entity references)
                    CASE WHEN ar.source_entity_id = gt.entity_id THEN ar.target_entity_id ELSE NULL END
                ELSE NULL
            END as entity_id,
            gt.depth + 1 as depth,
            gt.path || CASE
                WHEN ar.source_entity_id = gt.entity_id THEN ar.target_entity_id
                WHEN ar.target_entity_id = gt.entity_id THEN ar.source_entity_id
                ELSE NULL
            END as path
        FROM GraphTraversal gt
        JOIN ActiveRelations ar ON (
            (ar.source_entity_id = gt.entity_id AND $5 = TRUE) OR
            (ar.target_entity_id = gt.entity_id AND $4 = TRUE)
        )
        WHERE gt.depth < $3
        AND (
            CASE
                WHEN ar.source_entity_id = gt.entity_id THEN ar.target_entity_id
                WHEN ar.target_entity_id = gt.entity_id THEN ar.source_entity_id
                ELSE NULL
            END
        ) != ALL(gt.path) -- Prevent cycles
        AND (
            CASE
                WHEN ar.source_entity_id = gt.entity_id THEN ar.target_entity_id
                WHEN ar.target_entity_id = gt.entity_id THEN ar.source_entity_id
                ELSE NULL
            END
        ) IS NOT NULL
    ),
    -- Get edges between traversed entities
    EdgeDetails AS (
        SELECT DISTINCT
            ar.source_entity_id as source_ref,
            ar.target_entity_id as target_ref
        FROM ActiveRelations ar
        JOIN GraphTraversal gt1 ON gt1.entity_id = ar.source_entity_id
        JOIN GraphTraversal gt2 ON gt2.entity_id = ar.target_entity_id
    )
    SELECT source_ref, target_ref
    FROM EdgeDetails
    """,

    case epg_pool:query(Worker, Query, [ObjectRef, Version, Depth, IncludeInbound, IncludeOutbound]) of
        {ok, _Columns, Rows} ->
            {EntityIds, Edges} = parse_graph_edges_result(Rows),
            {ok, {EntityIds, Edges}};
        {error, Reason} ->
            logger:error("Error in graph edge traversal for ~p: ~p", [ObjectRef, Reason]),
            {error, Reason}
    end.

parse_graph_edges_result(Rows) ->
    Edges = [
        #{
            source_ref => dmt_mapper:from_string(SourceRef),
            target_ref => dmt_mapper:from_string(TargetRef)
        }
     || {SourceRef, TargetRef} <- Rows
    ],

    EntityIds0 =
        [SourceRef || {SourceRef, _TargetRef} <- Rows] ++
            [TargetRef || {_SourceRef, TargetRef} <- Rows],
    EntityIds1 = ordsets:from_list(EntityIds0),

    {EntityIds1, Edges}.
