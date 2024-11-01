-module(dmt_integration_test_SUITE).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("damsel/include/dmsl_domain_thrift.hrl").
-include_lib("stdlib/include/assert.hrl").

%% API
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    end_per_testcase/2,
    all/0,
    groups/0
]).

-export([
    %%    UserOpManagement Tests
    create_user_op_test/1,
    get_user_op_test/1,
    delete_user_op_test/1
]).

-export([
    %% Repository Tests
    insert_object_forced_id_success_test/1,
    insert_object_sequence_id_success_test/1,
    insert_remove_referencing_object_success_test/1,
    update_object_success_test/1
]).

-export([
    %% RepositoryClient Tests
]).

%% Setup and Teardown

%% Initialize per suite
init_per_suite(Config) ->
    {Apps, _Ret} = dmt_ct_helper:start_apps([woody, scoper, epg_connector, dmt]),
    ApiClient = dmt_ct_helper:create_client(),
    [{client, ApiClient}, {apps, Apps} | Config].

%% Cleanup after suite
end_per_suite(_Config) ->
    dmt_ct_helper:cleanup_db(),
    ok.

%% Define all test cases
all() ->
    [
        {group, create_user_op_test},
        {group, repository_tests}
        %%        {group, repository_client_tests}
    ].

%% Define test groups
groups() ->
    [
        {create_user_op_test, [parallel], [
            create_user_op_test,
            get_user_op_test,
            delete_user_op_test
        ]},
        {repository_tests, [], [
            insert_object_forced_id_success_test,
            insert_object_sequence_id_success_test,
            insert_remove_referencing_object_success_test,
            update_object_success_test
        ]},
        {repository_client_tests, [], []}
    ].

init_per_group(repository_client_tests, C) ->
    Client = dmt_ct_helper:cfg(client, C),
    [
        {user_op_id, create_user_op(<<"repository_client_tests@tests">>, Client)}
        | C
    ];
init_per_group(_, C) ->
    C.

end_per_group(_, _C) ->
    ok.

end_per_testcase(_, _C) ->
    dmt_ct_helper:cleanup_db(),
    ok.

%% Test Cases

%% UserOpManagement Tests

create_user_op_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    UserOpParams = #domain_conf_v2_UserOpParams{
        email = <<"create_user_op_test@test">>,
        name = <<"some_name">>
    },

    {ok, _} = dmt_client:create_user_op(UserOpParams, Client).

get_user_op_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"get_user_op_test">>,

    UserOpID = create_user_op(Email, Client),

    {ok, #domain_conf_v2_UserOp{
        email = Email1
    }} = dmt_client:get_user_op(UserOpID, Client),
    ?assertEqual(Email, Email1).

delete_user_op_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"delete_user_op_test">>,

    UserOpID = create_user_op(Email, Client),

    {ok, ok} = dmt_client:delete_user_op(UserOpID, Client),

    {exception, #domain_conf_v2_UserOpNotFound{}} =
        dmt_client:get_user_op(UserOpID, Client).

%% Repository tests

insert_remove_referencing_object_success_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"insert_remove_referencing_object_success_test">>,
    UserOpID = create_user_op(Email, Client),

    Revision1 = 0,
    Commit1 = #domain_conf_v2_Commit{
        ops = [
            {insert, #domain_conf_v2_InsertOp{
                object =
                    {proxy, #domain_ProxyDefinition{
                        name = <<"proxy">>,
                        description = <<"proxy_description">>,
                        url = <<"http://someurl">>,
                        options = #{}
                    }}
            }}
        ]
    },
    {ok, #domain_conf_v2_CommitResponse{
        version = Revision2,
        new_objects = [
            {proxy, #domain_ProxyObject{
                ref = ProxyRef
            }}
        ]
    }} = dmt_client:commit(Revision1, Commit1, UserOpID, Client),

    Commit2 = #domain_conf_v2_Commit{
        ops = [
            {insert, #domain_conf_v2_InsertOp{
                object =
                    {provider, #domain_Provider{
                        name = <<"name">>,
                        description = <<"description">>,
                        proxy = #domain_Proxy{
                            ref = ProxyRef,
                            additional = #{}
                        }
                    }}
            }}
        ]
    },

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision3,
        new_objects = [
            {provider, #domain_ProviderObject{
                ref = _ProviderRef
            }}
        ]
    }} = dmt_client:commit(Revision2, Commit2, UserOpID, Client),

    %%  try to remove proxy
    Commit3 = #domain_conf_v2_Commit{
        ops = [
            {remove, #domain_conf_v2_RemoveOp{
                ref = {proxy, ProxyRef}
            }}
        ]
    },

    {ok, _} = dmt_client:commit(Revision3, Commit3, UserOpID, Client).

%% FIXME reference collecting doesn't work. Need to fix ASAP

%%
%%%%  try to remove provider
%%    Commit4 = #domain_conf_v2_Commit{
%%        ops = [
%%            {remove, #domain_conf_v2_RemoveOp{
%%                ref = {provider, ProviderRef}
%%            }}
%%        ]
%%    },
%%    {ok, #domain_conf_v2_CommitResponse{
%%        version = Revision4
%%    }} = dmt_client:commit(Revision3, Commit4, UserOpID, Client),
%%
%%%%  try to remove proxy again
%%    {ok, #domain_conf_v2_CommitResponse{}} =
%%        dmt_client:commit(Revision4, Commit3, UserOpID, Client).

insert_object_sequence_id_success_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"insert_object_sequence_id_success_test">>,
    UserOpID = create_user_op(Email, Client),

    %% Insert a test object
    Revision = 0,
    Category = #domain_Category{
        name = <<"name1">>,
        description = <<"description1">>
    },
    Commit = #domain_conf_v2_Commit{
        ops = [
            {insert, #domain_conf_v2_InsertOp{
                object = {category, Category}
            }},
            {insert, #domain_conf_v2_InsertOp{
                object = {category, Category}
            }}
        ]
    },

    {ok, #domain_conf_v2_CommitResponse{
        new_objects = [
            {category, #domain_CategoryObject{
                ref = #domain_CategoryRef{id = ID1}
            }},
            {category, #domain_CategoryObject{
                ref = #domain_CategoryRef{id = ID2}
            }}
        ]
    }} = dmt_client:commit(Revision, Commit, UserOpID, Client),

    ?assertMatch(true, is_in_sequence(ID1, ID2)).

is_in_sequence(N1, N2) when N1 + 1 =:= N2 ->
    true;
is_in_sequence(N1, N2) when N1 =:= N2 + 1 ->
    true;
is_in_sequence(N1, N2) ->
    {false, N1, N2}.

%% Test successful insert with forced id
insert_object_forced_id_success_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"insert_object_forced_id_success_test">>,
    UserOpID = create_user_op(Email, Client),

    %% Insert a test object
    Revision = 0,
    CategoryRef = #domain_CategoryRef{id = 1337},
    ForcedRef = {category, CategoryRef},
    Category = #domain_Category{
        name = <<"name">>,
        description = <<"description">>
    },
    Commit = #domain_conf_v2_Commit{
        ops = [
            {insert, #domain_conf_v2_InsertOp{
                object =
                    {category, Category},
                force_ref = ForcedRef
            }}
        ]
    },

    {ok, #domain_conf_v2_CommitResponse{
        new_objects = NewObjectsSet
    }} = dmt_client:commit(Revision, Commit, UserOpID, Client),

    [
        {category, #domain_CategoryObject{
            ref = Ref,
            data = Category
        }}
    ] = ordsets:to_list(NewObjectsSet),
    ?assertMatch(CategoryRef, Ref).

update_object_success_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"insert_remove_referencing_object_success_test">>,
    UserOpID = create_user_op(Email, Client),

    Revision1 = 0,
    Commit1 = #domain_conf_v2_Commit{
        ops = [
            {insert, #domain_conf_v2_InsertOp{
                object =
                    {proxy, #domain_ProxyDefinition{
                        name = <<"proxy">>,
                        description = <<"proxy_description">>,
                        url = <<"http://someurl">>,
                        options = #{}
                    }}
            }}
        ]
    },
    {ok, #domain_conf_v2_CommitResponse{
        version = Revision2,
        new_objects = [
            {proxy, #domain_ProxyObject{
                ref = ProxyRef
            }}
        ]
    }} = dmt_client:commit(Revision1, Commit1, UserOpID, Client),

    NewObject =
        {proxy, #domain_ProxyObject{
            ref = ProxyRef,
            data = #domain_ProxyDefinition{
                name = <<"proxy2">>,
                description = <<"proxy_description2">>,
                url = <<"http://someurl">>,
                options = #{}
            }
        }},

    Commit2 = #domain_conf_v2_Commit{
        ops = [
            {update, #domain_conf_v2_UpdateOp{
                targeted_ref = {proxy, ProxyRef},
                new_object = NewObject
            }}
        ]
    },

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision3
    }} = dmt_client:commit(Revision2, Commit2, UserOpID, Client),

    {ok, #domain_conf_v2_VersionedObject{
        object = NewObject
    }} = dmt_client:checkout_object({version, Revision3}, {proxy, ProxyRef}, Client).

%% RepositoryClient Tests

%% GetLocalVersions

%% GetGlobalVersions

create_user_op(Email, Client) ->
    %%  Create UserOp
    UserOpParams = #domain_conf_v2_UserOpParams{
        email = Email,
        name = <<"some_name">>
    },

    {ok, #domain_conf_v2_UserOp{
        id = UserOpID
    }} = dmt_client:create_user_op(UserOpParams, Client),
    UserOpID.
