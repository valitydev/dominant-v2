-module(dmt_author).
-typing([eqwalizer]).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

-define(POOL_NAME, author_pool).

-export([
    insert/2,
    get/1,
    get_by_email/1,
    delete/1
]).

-type author_id() :: binary().
-type name() :: binary().
-type email() :: binary().
-type author() :: dmsl_domain_conf_v2_thrift:'Author'().

-export_type([author_id/0, name/0, email/0, author/0]).

-spec insert(name(), email()) ->
    {ok, author_id()} | {ok, {already_exists, author_id()}} | {error, unknown}.
insert(Name, Email) ->
    dmt_author_database:insert(?POOL_NAME, Name, Email).

-spec get(author_id()) -> {ok, author()} | {error, author_not_found | term()}.
get(AuthorID) ->
    dmt_author_database:get(?POOL_NAME, AuthorID).

-spec get_by_email(email()) -> {ok, author()} | {error, author_not_found | term()}.
get_by_email(Email) ->
    dmt_author_database:get_by_email(?POOL_NAME, Email).

-spec delete(author_id()) -> ok | {error, author_not_found | term()}.
delete(AuthorID) ->
    dmt_author_database:delete(?POOL_NAME, AuthorID).
