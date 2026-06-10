-module(dmt_author_database).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("epgsql/include/epgsql.hrl").

%% API
-export([
    insert/3,
    get/2,
    get_by_email/2,
    delete/2,
    list/3,
    search/3
]).

-spec insert(dmt_database:worker(), dmt_author:name(), dmt_author:email()) ->
    {ok, dmt_author:author_id()} | {ok, {already_exists, dmt_author:author_id()}} | {error, unknown}.
insert(Worker, Name, Email) ->
    Sql = """
    INSERT INTO author (name, email)
    VALUES ($1, $2)
    ON CONFLICT (email) DO UPDATE
    SET name = author.name
    RETURNING id, (xmax = 0) AS is_new
    """,
    Params = [Name, Email],
    case epg_pool:query(Worker, Sql, Params) of
        {ok, 1, _Columns, [{ID, true}]} ->
            {ok, ID};
        {ok, 1, _Columns, [{ID, false}]} ->
            {ok, {already_exists, ID}};
        {error, Error} ->
            logger:error("Insert Author error Name: ~p Email ~p Error ~p", [Name, Email, Error]),
            {error, unknown}
    end.

-spec get(dmt_database:worker(), dmt_author:author_id()) ->
    {ok, dmt_author:author()} | {error, author_not_found | dmt_database:db_error()}.
get(Worker, AuthorID) ->
    case is_uuid(AuthorID) of
        true ->
            get_(Worker, AuthorID);
        false ->
            {error, author_not_found}
    end.

-spec get_(dmt_database:worker(), dmt_author:author_id()) ->
    {ok, dmt_author:author()} | {error, author_not_found | dmt_database:db_error()}.
get_(Worker, AuthorID) ->
    Sql = """
    SELECT id, name, email
    FROM author
    WHERE id = $1::uuid
    """,
    Params = [AuthorID],
    case epg_pool:query(Worker, Sql, Params) of
        {ok, _Columns, [{ID, Name, Email}]} ->
            {ok, #domain_conf_v2_Author{
                id = ID,
                name = Name,
                email = Email
            }};
        {ok, _, []} ->
            {error, author_not_found};
        {error, Reason} ->
            {error, Reason}
    end.

-spec get_by_email(dmt_database:worker(), dmt_author:email()) ->
    {ok, dmt_author:author()} | {error, author_not_found | dmt_database:db_error()}.
get_by_email(Worker, Email) ->
    Sql = """
    SELECT id, name, email
    FROM author
    WHERE email = $1
    """,
    Params = [Email],
    case epg_pool:query(Worker, Sql, Params) of
        {ok, _Columns, [{ID, Name, Email}]} ->
            {ok, #domain_conf_v2_Author{
                id = ID,
                name = Name,
                email = Email
            }};
        {ok, _, []} ->
            {error, author_not_found};
        {error, Reason} ->
            {error, Reason}
    end.

-spec delete(dmt_database:worker(), dmt_author:author_id()) ->
    ok | {error, author_not_found | dmt_database:db_error()}.
delete(Worker, AuthorID) ->
    case is_uuid(AuthorID) of
        true ->
            delete_(Worker, AuthorID);
        false ->
            {error, author_not_found}
    end.

-spec delete_(dmt_database:worker(), dmt_author:author_id()) ->
    ok | {error, author_not_found | dmt_database:db_error()}.
delete_(Worker, AuthorID) ->
    Sql = """
    DELETE FROM author
    WHERE id = $1::uuid
    """,
    Params = [AuthorID],
    case epg_pool:query(Worker, Sql, Params) of
        {ok, 1} ->
            ok;
        {ok, 0} ->
            {error, author_not_found};
        {error, Reason} ->
            {error, Reason}
    end.

-spec list(dmt_database:worker(), pos_integer(), non_neg_integer()) ->
    {ok, [dmt_author:author()]} | {error, dmt_database:db_error()}.
list(Worker, Limit, Offset) ->
    Sql = """
    SELECT id, name, email
    FROM author
    ORDER BY name
    LIMIT $1 OFFSET $2
    """,
    Params = [Limit, Offset],
    case epg_pool:query(Worker, Sql, Params) of
        {ok, _Columns, Rows} ->
            Authors = [
                #domain_conf_v2_Author{
                    id = ID,
                    name = Name,
                    email = Email
                }
             || {ID, Name, Email} <- Rows
            ],
            {ok, Authors};
        {error, Reason} ->
            {error, Reason}
    end.

-spec search(dmt_database:worker(), binary(), pos_integer()) ->
    {ok, [dmt_author:author()]} | {error, dmt_database:db_error()}.
search(Worker, SearchTerm, Limit) ->
    Sql = """
    SELECT id, name, email FROM author
    WHERE name ILIKE $1 OR email ILIKE $1
    ORDER BY name
    LIMIT $2
    """,
    SearchPattern = <<"%", SearchTerm/binary, "%">>,
    Params = [SearchPattern, Limit],
    case epg_pool:query(Worker, Sql, Params) of
        {ok, _Columns, Rows} ->
            Authors = [
                #domain_conf_v2_Author{
                    id = ID,
                    name = Name,
                    email = Email
                }
             || {ID, Name, Email} <- Rows
            ],
            {ok, Authors};
        {error, Reason} ->
            {error, Reason}
    end.

%% Internal functions

-spec is_uuid(binary() | string()) -> boolean().
is_uuid(UUID) ->
    try uuid:string_to_uuid(UUID) of
        _UUID ->
            true
    catch
        exit:badarg ->
            false
    end.
