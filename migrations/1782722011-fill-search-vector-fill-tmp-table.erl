-module('1782722011-fill-search-vector-fill-tmp-table').

-export([perform/2]).

-define(TMP_TABLE, "tmp_entity_search_vector").
-define(BATCH_SIZE, 500).

perform(Conn, _MigrationOpts) ->
    ok = create_tmp_table(Conn),
    ObjectsCount = count_objects(Conn),
    BatchesCount = round(math:ceil(ObjectsCount / ?BATCH_SIZE)),
    ok = lists:foreach(
        fun(N) ->
            Objects0 = collect_objects(Conn, N * ?BATCH_SIZE, ?BATCH_SIZE),
            Objects1 = lists:map(fun form_search_vector/1, Objects0),
            insert_objects_into_buffer(Conn, Objects1)
        end,
        lists:seq(0, BatchesCount - 1)
    ),
    ok = update_entities(Conn),
    ok = drop_tmp_table(Conn).

%%

create_tmp_table(Conn) ->
    %% NOTE TEMP table: dropped automatically with the session, so a failed
    %% migration run can never leave a stale table behind that would make the
    %% next run fail at CREATE TABLE. Temporary tables cannot carry foreign
    %% keys to permanent tables, hence no REFERENCES on version.
    Query = io_lib:format(
        """
        CREATE TEMP TABLE ~s (
            id TEXT NOT NULL,
            version BIGINT NOT NULL,
            search_vector tsvector,
            PRIMARY KEY (id, version)
        )
        """,
        [?TMP_TABLE]
    ),
    case epgsql:squery(Conn, Query) of
        {error, Error} -> erlang:throw(Error);
        _ -> ok
    end.

update_entities(Conn) ->
    Query = io_lib:format(
        """
        UPDATE entity
        SET search_vector = tmp.search_vector
        FROM (SELECT id, version, search_vector FROM ~s) AS tmp
        WHERE entity.id = tmp.id AND entity.version = tmp.version
        """,
        [?TMP_TABLE]
    ),
    case epgsql:squery(Conn, Query) of
        {error, Error} -> erlang:throw(Error);
        _ -> ok
    end.

drop_tmp_table(Conn) ->
    Query = io_lib:format("DROP TABLE ~s", [?TMP_TABLE]),
    case epgsql:squery(Conn, Query) of
        {error, Error} -> erlang:throw(Error);
        _ -> ok
    end.

count_objects(Conn) ->
    case epgsql:squery(Conn, "SELECT COUNT(*) FROM entity") of
        {ok, _Cols, [{Count}]} -> binary_to_integer(Count);
        {error, Error} -> erlang:throw(Error)
    end.

collect_objects(Conn, Offset, Limit) ->
    Query = """
    SELECT id, version, entity_type, data
    FROM entity
    ORDER BY id, version ASC
    LIMIT $1 OFFSET $2
    """,
    case epgsql:equery(Conn, Query, [Limit, Offset]) of
        {ok, _Cols, Objects} -> Objects;
        {error, Error} -> erlang:throw(Error)
    end.

form_search_vector({ID, Version, Type, Data}) ->
    try
        SearchVector = dmt_mapper:to_text_search_vector(Type, Data),
        [ID, Version, SearchVector]
    catch
        Class:Reason ->
            %% One bad row aborts the whole migration; report which one so a
            %% production backfill failure is diagnosable.
            erlang:throw(
                {search_vector_formation_failed, #{
                    id => ID,
                    version => Version,
                    class => Class,
                    reason => Reason
                }}
            )
    end.

insert_objects_into_buffer(_Conn, []) ->
    ok;
insert_objects_into_buffer(Conn, Objects) ->
    QueryHead = io_lib:format("INSERT INTO ~s (id, version, search_vector) VALUES ", [?TMP_TABLE]),
    Values = lists:join(
        $,,
        lists:map(
            fun({I, [_ID, _Version, _SearchVector]}) ->
                PH = [ph(I, 1), ph(I, 2), ["to_tsvector('multilingual',", ph(I, 3), ")"]],
                [$(, lists:join($,, PH), $)]
            end,
            lists:zip(lists:seq(1, length(Objects)), Objects)
        )
    ),
    Params = lists:append(Objects),
    case epgsql:equery(Conn, [QueryHead | Values], Params) of
        {error, Error} -> erlang:throw(Error);
        _ -> ok
    end.

%% Placeholder helper
ph(I, N) ->
    [$$, integer_to_binary((I - 1) * 3 + N)].
