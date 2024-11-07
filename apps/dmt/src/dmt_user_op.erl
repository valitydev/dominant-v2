-module(dmt_user_op).

%% Existing includes and exports
-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("epgsql/include/epgsql.hrl").

-define(POOL_NAME, user_op_pool).

-export([
    insert_user/2,
    get_user/1,
    delete_user/1
]).

%% Insert a new user
insert_user(Name, Email) ->
    Sql = "INSERT INTO op_user (name, email) VALUES ($1, $2) returning id",
    Params = [Name, Email],
    case epg_pool:query(?POOL_NAME, Sql, Params) of
        {ok, 1, _Columns, [{ID}]} ->
            {ok, ID};
        {error, #error{code = <<"23505">>}} ->
            {error, already_exists};
        {error, Error} ->
            logger:error("Insert UserOp error Name: ~p Email ~p Error ~p", [Name, Email, Error]),
            {error, unknown}
    end.

%% Retrieve a user by ID
get_user(UserOpID) ->
    case is_uuid(UserOpID) of
        true ->
            get_user_(UserOpID);
        false ->
            {error, user_not_found}
    end.

get_user_(UserOpID) ->
    Sql = "SELECT id, name, email FROM op_user WHERE id = $1::uuid",
    Params = [UserOpID],
    case epg_pool:query(?POOL_NAME, Sql, Params) of
        {ok, _Columns, [{ID, Name, Email}]} ->
            {ok, #domain_conf_v2_UserOp{id = ID, name = Name, email = Email}};
        {ok, _, []} ->
            {error, user_not_found};
        {error, Reason} ->
            {error, Reason}
    end.

%% Delete a user by ID
delete_user(UserOpID) ->
    case is_uuid(UserOpID) of
        true ->
            delete_user_(UserOpID);
        false ->
            {error, user_not_found}
    end.

delete_user_(UserOpID) ->
    Sql = "DELETE FROM op_user WHERE id = $1::uuid",
    Params = [UserOpID],
    case epg_pool:query(?POOL_NAME, Sql, Params) of
        {ok, _, Result} when Result =:= [] ->
            {error, user_not_found};
        {ok, 1} ->
            ok;
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
