-module(dmt_client).

-include_lib("opentelemetry_api/include/otel_tracer.hrl").
-include_lib("opentelemetry_api/include/opentelemetry.hrl").

%% API
-export([
    checkout_object/3,
    get_local_versions/2,
    get_all_objects_history/2,
    get_latest_version/1,
    commit/4,
    search_objects/2
]).

-export([
    create_author/2,
    get_author/2,
    delete_author/2
]).

checkout_object(VersionRef, ObjectRef, Client) ->
    Args = [VersionRef, ObjectRef],
    dmt_client_api:call(repository_client, 'CheckoutObject', Args, Client).

get_local_versions(Request, Client) ->
    Args = [Request],
    dmt_client_api:call(repository, 'GetLocalVersions', Args, Client).

get_all_objects_history(Request, Client) ->
    Args = [Request],
    dmt_client_api:call(repository, 'GetAllObjectsHistory', Args, Client).

get_latest_version(Client) ->
    dmt_client_api:call(repository, 'GetLatestVersion', [], Client).

commit(Version, Commit, Author, Client) ->
    Args = [Version, Commit, Author],
    dmt_client_api:call(repository, 'Commit', Args, Client).

search_objects(Request, Client) ->
    Args = [Request],
    dmt_client_api:call(repository, 'SearchObjects', Args, Client).

create_author(Params, Client) ->
    Args = [Params],
    dmt_client_api:call(author, 'Create', Args, Client).

get_author(ID, Client) ->
    Args = [ID],
    dmt_client_api:call(author, 'Get', Args, Client).

delete_author(ID, Client) ->
    Args = [ID],
    dmt_client_api:call(author, 'Delete', Args, Client).
