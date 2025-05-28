-module(dmt_author).

%% Public API

-define(POOL_NAME, author_pool).

-export([
    insert/2,
    get/1,
    get_by_email/1,
    delete/1
]).

%% Optional: Extended API (can be uncommented if these functions should be exposed)
%% -export([
%%     list/2,
%%     search/2
%% ]).

insert(Name, Email) ->
    dmt_author_database:insert(?POOL_NAME, Name, Email).

get(AuthorID) ->
    dmt_author_database:get(?POOL_NAME, AuthorID).

get_by_email(Email) ->
    dmt_author_database:get_by_email(?POOL_NAME, Email).

delete(AuthorID) ->
    dmt_author_database:delete(?POOL_NAME, AuthorID).

%% Optional: Extended functionality that could be exposed if needed
%% @doc Lists authors with pagination
%% -spec list(Limit :: pos_integer(), Offset :: non_neg_integer()) ->
%%     {ok, [#domain_conf_v2_Author{}]} | {error, term()}.
%% list(Limit, Offset) ->
%%     dmt_author_database:list(?POOL_NAME, Limit, Offset).

%% @doc Searches for authors by name or email
%% -spec search(SearchTerm :: binary(), Limit :: pos_integer()) ->
%%     {ok, [#domain_conf_v2_Author{}]} | {error, term()}.
%% search(SearchTerm, Limit) ->
%%     dmt_author_database:search(?POOL_NAME, SearchTerm, Limit).
