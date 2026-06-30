-module('1782722011-fill-search-vector-fill-tmp-table').

-export([perform/2]).

-define(BATCH_SIZE, 100).

perform(Conn, _MigrationOpts) ->
    ObjectsCount = count_objects(Conn),
    BatchesCount = round(math:ceil(ObjectsCount / ?BATCH_SIZE)),
    ok = lists:foreach(
        fun(N) ->
            Objects0 = collect_objects(Conn, Offset, ?BATCH_SIZE),
            Objects1 = lists:map(fun form_search_vector/1, Objects0),
            insert_objects_into_buffer(Conn, Objects1)
        end,
        lists:seq(0, BatchesCount - 1)
    ).

%%

count_objects(Conn) ->
    {ok, _Cols, Count} = epgsql:squery(Conn, "SELECT COUNT(*) FROM entity"),
    binary_to_integer(Count).

collect_objects(Conn, Offset, Limit) ->
    Query = """
    SELECT id, version, entity_type, data
    FROM entity
    ORDER BY id, version ASC
    LIMIT $1 OFFSET $2
    """,
    {ok, _Cols, Objects} = epgsql:equery(Conn, Query, [Limit, Offset]),
    Objects.

form_search_vector({ID, Version, _Type, Data}) ->
    SearchVector = dmt_mapper:extract_searchable_text_from_term(jsx:decode(Data)),
    [ID, Version, SearchVector].

insert_objects_into_buffer(Conn, Objects) ->
    epgsql:execute_batch(
        """
        INSERT INTO tmp_entity_search_vector (id, version, search_vector)
        VALUES ($1, $2, to_tsvector('multilingual', $3))
        """,
        Objects
    ).
