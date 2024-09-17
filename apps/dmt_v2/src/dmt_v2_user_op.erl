-module(dmt_v2_user_op).

%% Existing includes and exports
-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("epgsql/include/epgsql.hrl").

-define(POOL_NAME, user_op_pool).

-export([
    insert_user/3,
    get_user/1,
    delete_user/1
]).

%% Insert a new user
insert_user(UserOpID, Name, Email) ->
    Sql = "INSERT INTO op_user (id, name, email) VALUES ($1::uuid, $2, $3)",
    Params = [UserOpID, Name, Email],
    case epgsql_pool:query(?POOL_NAME, Sql, Params) of
        {ok, _, _} ->
            ok;
        {error, {error, constraint_violation, _, "op_user_id_key", _}} ->
            {error, already_exists};
        {error, Reason} ->
            {error, Reason}
    end.

%% Retrieve a user by ID
get_user(UserOpID) ->
    Sql = "SELECT id, name, email FROM op_user WHERE id = $1::uuid",
    Params = [UserOpID],
    case epgsql_pool:query(?POOL_NAME, Sql, Params) of
        {ok, _Columns, [{ID, Name, Email}]} ->
            {ok, #domain_conf_v2_UserOp{id = ID, name = Name, email = Email}};
        {ok, _, []} ->
            {error, user_not_found};
        {error, Reason} ->
            {error, Reason}
    end.

%% Delete a user by ID
delete_user(UserOpID) ->
    Sql = "DELETE FROM op_user WHERE id = $1::uuid",
    Params = [UserOpID],
    case epgsql_pool:query(?POOL_NAME, Sql, Params) of
        {ok, _, Result} when Result =:= [] ->
            {error, user_not_found};
        {ok, _, _} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
