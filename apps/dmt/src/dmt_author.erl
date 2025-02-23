-module(dmt_author).

%% Existing includes and exports
-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("epgsql/include/epgsql.hrl").

-define(POOL_NAME, author_pool).

-export([
    insert/2,
    get/1,
    delete/1
]).

insert(Name, Email) ->
    Sql = "INSERT INTO author (name, email) VALUES ($1, $2) returning id",
    Params = [Name, Email],
    case epg_pool:query(?POOL_NAME, Sql, Params) of
        {ok, 1, _Columns, [{ID}]} ->
            {ok, ID};
        {error, #error{code = <<"23505">>}} ->
            {error, already_exists};
        {error, Error} ->
            logger:error("Insert Author error Name: ~p Email ~p Error ~p", [Name, Email, Error]),
            {error, unknown}
    end.

get(AuthorID) ->
    case is_uuid(AuthorID) of
        true ->
            get_(AuthorID);
        false ->
            {error, author_not_found}
    end.

get_(AuthorID) ->
    Sql = "SELECT id, name, email FROM author WHERE id = $1::uuid",
    Params = [AuthorID],
    case epg_pool:query(?POOL_NAME, Sql, Params) of
        {ok, _Columns, [{ID, Name, Email}]} ->
            {ok, #domain_conf_v2_Author{id = ID, name = Name, email = Email}};
        {ok, _, []} ->
            {error, author_not_found};
        {error, Reason} ->
            {error, Reason}
    end.

delete(AuthorID) ->
    case is_uuid(AuthorID) of
        true ->
            delete_(AuthorID);
        false ->
            {error, author_not_found}
    end.

delete_(AuthorID) ->
    Sql = "DELETE FROM author WHERE id = $1::uuid",
    Params = [AuthorID],
    case epg_pool:query(?POOL_NAME, Sql, Params) of
        {ok, _, Result} when Result =:= [] ->
            {error, author_not_found};
        {ok, 1} ->
            ok;
        {ok, 0} ->
            {error, author_not_found};
        {error, Reason} ->
            {error, Reason}
    end.

is_uuid(UUID) ->
    try uuid:string_to_uuid(UUID) of
        _UUID ->
            true
    catch
        exit:badarg ->
            false
    end.
