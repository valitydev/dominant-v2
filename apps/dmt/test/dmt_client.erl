-module(dmt_client).

-include_lib("opentelemetry_api/include/otel_tracer.hrl").
-include_lib("opentelemetry_api/include/opentelemetry.hrl").

%% API
-export([
    checkout_object/3,
    checkout_objects/3,
    checkout_snapshot/2,
    get_object_history/3,
    get_all_objects_history/2,
    get_latest_version/1,
    commit/4,
    search_objects/2,
    search_full_objects/2
]).

-export([
    create_author/2,
    get_author/2,
    delete_author/2,
    get_author_by_email/2
]).

checkout_object(VersionRef, ObjectRef, Client) ->
    Args = [VersionRef, ObjectRef],
    dmt_client_api:call(repository_client, 'CheckoutObject', Args, Client).

checkout_objects(VersionRef, ObjectRefs, Client) ->
    Args = [VersionRef, ObjectRefs],
    dmt_client_api:call(repository_client, 'CheckoutObjects', Args, Client).

checkout_snapshot(VersionRef, Client) ->
    Args = [VersionRef],
    dmt_client_api:call(repository_client, 'CheckoutSnapshot', Args, Client).

get_object_history(Ref, Request, Client) ->
    Args = [Ref, Request],
    dmt_client_api:call(repository, 'GetObjectHistory', Args, Client).

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

search_full_objects(Request, Client) ->
    Args = [Request],
    dmt_client_api:call(repository, 'SearchFullObjects', Args, Client).

create_author(Params, Client) ->
    Args = [Params],
    dmt_client_api:call(author, 'Create', Args, Client).

get_author(ID, Client) ->
    Args = [ID],
    dmt_client_api:call(author, 'Get', Args, Client).

get_author_by_email(Email, Client) ->
    Args = [Email],
    dmt_client_api:call(author, 'GetByEmail', Args, Client).

delete_author(ID, Client) ->
    Args = [ID],
    dmt_client_api:call(author, 'Delete', Args, Client).
