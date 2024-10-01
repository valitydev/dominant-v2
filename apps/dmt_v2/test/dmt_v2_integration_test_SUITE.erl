-module(dmt_v2_integration_test_SUITE).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("damsel/include/dmsl_domain_thrift.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

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
    insert_object_forced_id_success_test/1
]).

-export([
    %% RepositoryClient Tests
    test_checkout_object_success/1
]).

%% Setup and Teardown

%% Initialize per suite
init_per_suite(Config) ->
    {Apps, _Ret} = dmt_v2_ct_helper:start_apps([woody, scoper, epg_connector, dmt_v2]),
    ApiClient = dmt_v2_ct_helper:create_client(),
    [{client, ApiClient}, {apps, Apps} | Config].

%% Cleanup after suite
end_per_suite(_Config) ->
    dmt_v2_ct_helper:cleanup_db(),
    ok.

%% Define all test cases
all() ->
    [
        {group, create_user_op_test},
        {group, repository_tests},
        {group, repository_client_tests}
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
            insert_object_forced_id_success_test
        ]},
        {repository_client_tests, [], [
            test_checkout_object_success
        ]}
    ].

init_per_group(repository_client_tests, C) ->
    Client = dmt_v2_ct_helper:cfg(client, C),
    [
        {user_op_id, create_user_op(<<"repository_client_tests@tests">>, Client)}
        | C
    ];
init_per_group(_, C) ->
    C.

end_per_group(_, _C) ->
    ok.

end_per_testcase(_, _C) ->
    dmt_v2_ct_helper:cleanup_db(),
    ok.

%% Test Cases

%% UserOpManagement Tests

create_user_op_test(Config) ->
    Client = dmt_v2_ct_helper:cfg(client, Config),

    UserOpParams = #domain_conf_v2_UserOpParams{
        email = <<"create_user_op_test@test">>,
        name = <<"some_name">>
    },

    {ok, _} = dmt_v2_client:create_user_op(UserOpParams, Client).

get_user_op_test(Config) ->
    Client = dmt_v2_ct_helper:cfg(client, Config),

    Email = <<"get_user_op_test">>,

    UserOpID = create_user_op(Email, Client),

    {ok, #domain_conf_v2_UserOp{
        email = Email1
    }} = dmt_v2_client:get_user_op(UserOpID, Client),
    ?assertEqual(Email, Email1).

delete_user_op_test(Config) ->
    Client = dmt_v2_ct_helper:cfg(client, Config),

    Email = <<"delete_user_op_test">>,

    UserOpID = create_user_op(Email, Client),

    {ok, ok} = dmt_v2_client:delete_user_op(UserOpID, Client),

    {exception, #domain_conf_v2_UserOpNotFound{}} =
        dmt_v2_client:get_user_op(UserOpID, Client).

%% RepositoryClient Tests

%% Test successful CheckoutObject
insert_object_forced_id_success_test(Config) ->
    Client = dmt_v2_ct_helper:cfg(client, Config),

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
    }} = dmt_v2_client:commit(Revision, Commit, UserOpID, Client),

    [
        {category, #domain_CategoryObject{
            ref = Ref,
            data = Category
        }}
    ] = ordsets:to_list(NewObjectsSet),
    ?assertMatch(CategoryRef, Ref).

test_checkout_object_success(Config) ->
    Client = dmt_v2_ct_helper:cfg(client, Config),

    %%  Create UserOp
    UserOpParams = #domain_conf_v2_UserOpParams{
        email = <<"some@email">>,
        name = <<"some_name">>
    },

    {ok, #domain_conf_v2_UserOp{
        id = UserOpID
    }} = dmt_v2_client:create_user_op(UserOpParams, Client),

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
        version = NewRevision,
        new_objects = NewObjectsSet
    }} = dmt_v2_client:commit(Revision, Commit, UserOpID, Client),

    [
        {category, #domain_CategoryObject{
            ref = Ref,
            data = Category
        }} = CategoryObject
    ] = ordsets:to_list(NewObjectsSet),
    ?assertMatch(CategoryRef, Ref),

    %%  Check that it was added
    {ok, #domain_conf_v2_VersionedObject{
        global_version = NewRevision,
        object = CategoryObject
    }} = dmt_v2_client:checkout_object({version, NewRevision}, ForcedRef, Client).

create_user_op(Email, Client) ->
    %%  Create UserOp
    UserOpParams = #domain_conf_v2_UserOpParams{
        email = Email,
        name = <<"some_name">>
    },

    {ok, #domain_conf_v2_UserOp{
        id = UserOpID
    }} = dmt_v2_client:create_user_op(UserOpParams, Client),
    UserOpID.
