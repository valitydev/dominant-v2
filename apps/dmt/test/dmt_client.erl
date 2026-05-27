-module(dmt_client).

%% API
-export([
    checkout_object/3,
    checkout_object_with_references/3,
    checkout_objects/3,
    checkout_snapshot/2,
    get_object_history/3,
    get_all_objects_history/2,
    get_latest_version/1,
    commit/4,
    search_objects/2,
    search_full_objects/2,
    get_related_graph/2,
    get_multiple_related_graph/2,
    search_related_graph/2
]).

-export([
    create_author/2,
    get_author/2,
    delete_author/2,
    get_author_by_email/2
]).

-type client() :: dmt_client_api:t().
-type call_result() :: {ok, term()} | {exception, term()} | {error, term()}.

-export_type([
    client/0, call_result/0
]).

-spec checkout_object(
    dmsl_domain_conf_v2_thrift:'VersionReference'(),
    dmsl_domain_thrift:'Reference'(),
    client()
) -> call_result().
checkout_object(VersionRef, ObjectRef, Client) ->
    Args = [VersionRef, ObjectRef],
    dmt_client_api:call(repository_client, 'CheckoutObject', Args, Client).

-spec checkout_object_with_references(
    dmsl_domain_conf_v2_thrift:'VersionReference'(),
    dmsl_domain_thrift:'Reference'(),
    client()
) -> call_result().
checkout_object_with_references(VersionRef, ObjectRef, Client) ->
    Args = [VersionRef, ObjectRef],
    dmt_client_api:call(repository_client, 'CheckoutObjectWithReferences', Args, Client).

%% NOTE: ObjectRefs are typed loosely (`[term()]`) so test suites can pass
%% deliberately malformed refs to exercise error paths server-side.
-spec checkout_objects(
    dmsl_domain_conf_v2_thrift:'VersionReference'(),
    [term()],
    client()
) -> call_result().
checkout_objects(VersionRef, ObjectRefs, Client) ->
    Args = [VersionRef, ObjectRefs],
    dmt_client_api:call(repository_client, 'CheckoutObjects', Args, Client).

-spec checkout_snapshot(dmsl_domain_conf_v2_thrift:'VersionReference'(), client()) -> call_result().
checkout_snapshot(VersionRef, Client) ->
    Args = [VersionRef],
    dmt_client_api:call(repository_client, 'CheckoutSnapshot', Args, Client).

-spec get_object_history(dmsl_domain_thrift:'Reference'(), term(), client()) -> call_result().
get_object_history(Ref, Request, Client) ->
    Args = [Ref, Request],
    dmt_client_api:call(repository, 'GetObjectHistory', Args, Client).

-spec get_all_objects_history(term(), client()) -> call_result().
get_all_objects_history(Request, Client) ->
    Args = [Request],
    dmt_client_api:call(repository, 'GetAllObjectsHistory', Args, Client).

-spec get_latest_version(client()) -> call_result().
get_latest_version(Client) ->
    dmt_client_api:call(repository, 'GetLatestVersion', [], Client).

-spec commit(
    dmsl_domain_conf_v2_thrift:'Version'(),
    term(),
    dmsl_domain_conf_v2_thrift:'AuthorID'(),
    client()
) -> call_result().
commit(Version, Commit, Author, Client) ->
    Args = [Version, Commit, Author],
    dmt_client_api:call(repository, 'Commit', Args, Client).

-spec search_objects(term(), client()) -> call_result().
search_objects(Request, Client) ->
    Args = [Request],
    dmt_client_api:call(repository, 'SearchObjects', Args, Client).

-spec search_full_objects(term(), client()) -> call_result().
search_full_objects(Request, Client) ->
    Args = [Request],
    dmt_client_api:call(repository, 'SearchFullObjects', Args, Client).

-spec create_author(dmsl_domain_conf_v2_thrift:'AuthorParams'(), client()) -> call_result().
create_author(Params, Client) ->
    Args = [Params],
    dmt_client_api:call(author, 'Create', Args, Client).

-spec get_author(dmsl_domain_conf_v2_thrift:'AuthorID'(), client()) -> call_result().
get_author(ID, Client) ->
    Args = [ID],
    dmt_client_api:call(author, 'Get', Args, Client).

-spec get_author_by_email(binary(), client()) -> call_result().
get_author_by_email(Email, Client) ->
    Args = [Email],
    dmt_client_api:call(author, 'GetByEmail', Args, Client).

-spec delete_author(dmsl_domain_conf_v2_thrift:'AuthorID'(), client()) -> call_result().
delete_author(ID, Client) ->
    Args = [ID],
    dmt_client_api:call(author, 'Delete', Args, Client).

-spec get_related_graph(term(), client()) -> call_result().
get_related_graph(Request, Client) ->
    Args = [Request],
    dmt_client_api:call(repository, 'GetRelatedGraph', Args, Client).

-spec get_multiple_related_graph(term(), client()) -> call_result().
get_multiple_related_graph(Request, Client) ->
    Args = [Request],
    dmt_client_api:call(repository, 'GetMultipleRelatedGraph', Args, Client).

-spec search_related_graph(term(), client()) -> call_result().
search_related_graph(Request, Client) ->
    Args = [Request],
    dmt_client_api:call(repository, 'SearchRelatedGraph', Args, Client).
