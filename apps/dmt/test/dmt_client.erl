-module(dmt_client).

-include_lib("opentelemetry_api/include/otel_tracer.hrl").
-include_lib("opentelemetry_api/include/opentelemetry.hrl").

%% API
-export([
    checkout_object/3,
    get_local_versions/2,
    get_global_versions/2,
    get_latest_global_version/1,
    commit/4
]).

-export([
    create_user_op/2,
    get_user_op/2,
    delete_user_op/2
]).

checkout_object(VersionRef, ObjectRef, Client) ->
    Args = [VersionRef, ObjectRef],
    dmt_client_api:call(repository_client, 'CheckoutObject', Args, Client).

get_local_versions(Request, Client) ->
    Args = [Request],
    dmt_client_api:call(repository_client, 'GetLocalVersions', Args, Client).

get_global_versions(Request, Client) ->
    Args = [Request],
    dmt_client_api:call(repository_client, 'GetGlobalVersions', Args, Client).

get_latest_global_version(Client) ->
    dmt_client_api:call(repository_client, 'GetLatestGlobalVersion', [], Client).

commit(Version, Commit, Author, Client) ->
    Args = [Version, Commit, Author],
    dmt_client_api:call(repository, 'Commit', Args, Client).

create_user_op(Params, Client) ->
    Args = [Params],
    dmt_client_api:call(user_op, 'Create', Args, Client).

get_user_op(ID, Client) ->
    Args = [ID],
    dmt_client_api:call(user_op, 'Get', Args, Client).

delete_user_op(ID, Client) ->
    Args = [ID],
    dmt_client_api:call(user_op, 'Delete', Args, Client).
