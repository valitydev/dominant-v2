-module(dmt_author_database).
-typing([eqwalizer]).

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

-type worker() :: atom().
-type author_id() :: dmt_author:author_id().
-type name() :: dmt_author:name().
-type email() :: dmt_author:email().
-type author() :: dmt_author:author().

-export_type([worker/0, author_id/0, name/0, email/0, author/0]).

-spec insert(worker(), name(), email()) ->
    {ok, author_id()} | {ok, {already_exists, author_id()}} | {error, unknown}.
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

-spec get(worker(), author_id()) -> {ok, author()} | {error, author_not_found | term()}.
get(Worker, AuthorID) ->
    case is_uuid(AuthorID) of
        true ->
            get_(Worker, AuthorID);
        false ->
            {error, author_not_found}
    end.

-spec get_(worker(), author_id()) -> {ok, author()} | {error, author_not_found | term()}.
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

-spec get_by_email(worker(), email()) -> {ok, author()} | {error, author_not_found | term()}.
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

-spec delete(worker(), author_id()) -> ok | {error, author_not_found | term()}.
delete(Worker, AuthorID) ->
    case is_uuid(AuthorID) of
        true ->
            delete_(Worker, AuthorID);
        false ->
            {error, author_not_found}
    end.

-spec delete_(worker(), author_id()) -> ok | {error, author_not_found | term()}.
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

-spec list(worker(), pos_integer(), non_neg_integer()) ->
    {ok, [author()]} | {error, term()}.
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

-spec search(worker(), binary(), pos_integer()) -> {ok, [author()]} | {error, term()}.
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

-spec is_uuid(term()) -> boolean().
is_uuid(<<UUID:32/binary>>) ->
    try_string_to_uuid(UUID);
is_uuid(<<UUID:36/binary>>) ->
    try_string_to_uuid(UUID);
is_uuid(_) ->
    false.

-spec try_string_to_uuid(uuid:uuid_string()) -> boolean().
try_string_to_uuid(UUID) ->
    try uuid:string_to_uuid(UUID) of
        _ ->
            true
    catch
        exit:badarg ->
            false
    end.
