-module(dmt_v2_integration_test_SUITE).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% API
-export([
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2,
  all/0,
  groups/0
]).

-export([
  %% RepositoryClient Tests
  test_checkout_object_success/1,
  test_checkout_object_not_found/1,
%%  test_get_local_versions_success/1,
%%  test_get_local_versions_pagination/1,
%%  test_get_global_versions_success/1,
%%  test_get_global_versions_pagination/1,

  %% Repository Tests
  test_commit_success/1,
  test_commit_conflict/1,
  test_commit_invalid/1,
  test_commit_obsolete_version/1,

  %% UserOpManagement Tests
  test_create_user_op_success/1,
  test_create_user_op_already_exists/1,
  test_get_user_op_success/1,
  test_get_user_op_not_found/1,
  test_delete_user_op_success/1,
  test_delete_user_op_not_found/1
]).

%% Setup and Teardown

%% Initialize per suite
init_per_suite(_Config) ->
  %% Start necessary applications
  ok = application:ensure_all_started(dmt_v2),
  ok.

%% Cleanup after suite
end_per_suite(_Config) ->
  %% Clean up, e.g., stop applications, clear test data
  ok = application:stop(dmt_v2),
  ok.

%% Initialize per testcase
init_per_testcase(_TestCase, _Config) ->
  %% Optionally, insert test data specific to each test
  ok.

%% Cleanup after testcase
end_per_testcase(_TestCase, _Config) ->
  %% Clean up test data, e.g., truncate tables
  ok = dmt_v2_repository:cleanup_db(),
  ok.

%% Define all test cases
all() ->
  [{group, repository_client_tests}, {group, repository_tests}, {group, user_op_management_tests}].

%% Define test groups
groups() ->
  [
    {repository_client_tests, [], [
      test_checkout_object_success,
      test_checkout_object_not_found
%%      test_get_local_versions_success,
%%      test_get_local_versions_pagination,
%%      test_get_global_versions_success,
%%      test_get_global_versions_pagination
    ]},
    {repository_tests, [], [
      test_commit_success,
      test_commit_conflict,
      test_commit_invalid,
      test_commit_obsolete_version
    ]},
    {user_op_management_tests, [], [
      test_create_user_op_success,
      test_create_user_op_already_exists,
      test_get_user_op_success,
      test_get_user_op_not_found,
      test_delete_user_op_success,
      test_delete_user_op_not_found
    ]}
  ].

%% Test Cases

%% RepositoryClient Tests

%% Test successful CheckoutObject
test_checkout_object_success(_Config) ->
  %% Insert a test object
  Ref = <<"test_object_ref">>,
  GlobalVS = 1,
  LocalVS = 1,
  CreatedAt = datetime_to_binary({{2023,1,1}, {12,0,0}}),
  Author = <<"author_uuid">>,
  Object = #domain_conf_v2_Object{
    id = Ref,
    type = <<"test_type">>,
    references = [],
    referenced_by = [],
    data = <<"test_data">>
  },
  %% Insert into object_versions
  {ok, _} = dmt_v2_repository:insert_test_object_versions(Ref, [
    {LocalVS, #domain_conf_v2_ObjectVersion{
      ref = Ref,
      global_version = GlobalVS,
      local_version = LocalVS,
      created_at = CreatedAt,
      author = Author
    }}
  ]),

  %% Perform CheckoutObject
  VersionRef = {global_vs, GlobalVS},
  case dmt_v2_repository_client:checkout_object(VersionRef, Ref) of
    {ok, #domain_conf_v2_VersionedObject{
      global_version = GV,
      local_version = LV,
      object = Obj,
      created_at = CA
    }} ->
      ?assertEqual(GlobalVS, GV),
      ?assertEqual(LocalVS, LV),
      ?assertEqual(Object, Obj),
      ?assertEqual(CreatedAt, CA);
    {error, Reason} ->
      ?fail("CheckoutObject failed with reason: ~p", [Reason])
  end.

%% Test CheckoutObject when object not found
test_checkout_object_not_found(_Config) ->
  Ref = <<"nonexistent_ref">>,
  VersionRef = {global_vs, 1},
  #domain_conf_v2_ObjectNotFound{} = dmt_v2_repository_client:checkout_object(VersionRef, Ref).

%% Repository Tests

%% Test successful Commit
test_commit_success(_Config) ->
  %% Prepare commit operations
  Version = 1,
  CreatedBy = <<"user_op_uuid">>,
  Commit = #domain_conf_v2_Commit{
    ops = [
      {insert, #domain_conf_v2_InsertOp{
        object = #domain_ReflessDomainObject{
          type = <<"test_type">>,
          references = [],
          data = <<"test_data">>
        },
        force_ref = undefined
      }}
    ]
  },
  %% Perform commit
  case dmt_v2_repository:commit(Version, Commit, CreatedBy) of
    {ok, NewVersion} ->
      ?assertGreaterThan(NewVersion, Version);
    {error, Reason} ->
      ?fail("Commit failed with reason: ~p", [Reason])
  end.

%% Test Commit with Conflict
test_commit_conflict(_Config) ->
  %% Insert an object with a specific ref
  Ref = <<"conflict_ref">>,
  Version = 1,
  CreatedBy = <<"user_op_uuid">>,
  Commit1 = #domain_conf_v2_Commit{
    ops = [
      {insert, #domain_conf_v2_InsertOp{
        object = #domain_ReflessDomainObject{
          type = <<"test_type">>,
          references = [],
          data = <<"test_data">>
        },
        force_ref = Ref
      }}
    ]
  },
  {ok, NewVersion1} = dmt_v2_repository:commit(Version, Commit1, CreatedBy),

  %% Attempt to insert the same object again to cause conflict
  Commit2 = #domain_conf_v2_Commit{
    ops = [
      {insert, #domain_conf_v2_InsertOp{
        object = #domain_ReflessDomainObject{
          type = <<"test_type">>,
          references = [],
          data = <<"test_data">>
        },
        force_ref = Ref
      }}
    ]
  },
  case dmt_v2_repository:commit(NewVersion1, Commit2, CreatedBy) of
    {error, {operation_error, {conflict, {object_already_exists, Ref}}}} ->
      ok;
    Other ->
      ?fail("Expected conflict error, got: ~p", [Other])
  end.

%% Test Commit with Invalid Operations
test_commit_invalid(_Config) ->
  %% Prepare commit operations with non-existent references
  Version = 2,
  CreatedBy = <<"user_op_uuid">>,
  Commit = #domain_conf_v2_Commit{
    ops = [
      {insert, #domain_conf_v2_InsertOp{
        object = #domain_ReflessDomainObject{
          type = <<"test_type">>,
          references = [<<"nonexistent_ref">>],
          data = <<"test_data">>
        },
        force_ref = undefined
      }}
    ]
  },
  %% Perform commit
  case dmt_v2_repository:commit(Version, Commit, CreatedBy) of
    {error, {operation_error, {invalid, {objects_not_exist, [<<"nonexistent_ref">>]}}}} ->
      ok;
    Other ->
      ?fail("Expected invalid operation error, got: ~p", [Other])
  end.

%% Test Commit with Obsolete Version
test_commit_obsolete_version(_Config) ->
  %% Assuming current global version is 3
  Version = 1,  %% obsolete
  CreatedBy = <<"user_op_uuid">>,
  Commit = #domain_conf_v2_Commit{
    ops = []
  },
  case dmt_v2_repository:commit(Version, Commit, CreatedBy) of
    {error, {operation_error, _}} ->
      ok;
    {error, #domain_conf_v2_ObsoleteCommitVersion{}} ->
      ok;
    Other ->
      ?fail("Expected obsolete version error, got: ~p", [Other])
  end.

%% UserOpManagement Tests

%% Test successful Create UserOp
test_create_user_op_success(_Config) ->
  Params = #domain_conf_v2_UserOpParams{
    email = <<"user@example.com">>,
    name = <<"Test User">>
  },
  case dmt_v2_user_op_handler:create(Params) of
    {ok, #domain_conf_v2_UserOp{id = ID, email = <<"user@example.com">>, name = <<"Test User">>}} ->
      ?assertNotEqual(<<"">>, ID);
    {error, Reason} ->
      ?fail("Create UserOp failed with reason: ~p", [Reason])
  end.

%% Test Create UserOp when already exists
test_create_user_op_already_exists(_Config) ->
  Params = #domain_conf_v2_UserOpParams{
    email = <<"duplicate@example.com">>,
    name = <<"Duplicate User">>
  },
  %% First creation
  {ok, #domain_conf_v2_UserOp{id = ID1, email = <<"duplicate@example.com">>, name = <<"Duplicate User">>}} =
    dmt_v2_user_op_handler:create(Params),

  %% Attempt duplicate creation
  {error, already_exists} =
    dmt_v2_user_op_handler:insert_user(ID1, <<"Duplicate User">>, <<"duplicate@example.com">>),

  ok.

%% Test successful Get UserOp
test_get_user_op_success(_Config) ->
  %% Create a UserOp
  Params = #domain_conf_v2_UserOpParams{
    email = <<"getuser@example.com">>,
    name = <<"Get User">>
  },
  {ok, #domain_conf_v2_UserOp{id = ID, email = <<"getuser@example.com">>, name = <<"Get User">>}} =
    dmt_v2_user_op_handler:create(Params),

  %% Retrieve the UserOp
  {ok, #domain_conf_v2_UserOp{id = ID, email = <<"getuser@example.com">>, name = <<"Get User">>}} =
    dmt_v2_user_op_handler:get(ID).

%% Test Get UserOp when not found
test_get_user_op_not_found(_Config) ->
  Ref = <<"nonexistent_user_op">>,
  case dmt_v2_user_op_handler:get(Ref) of
    {error, #domain_conf_v2_UserOpNotFound{}} ->
      ok;
    Other ->
      ?fail("Expected UserOpNotFound, got: ~p", [Other])
  end.

%% Test successful Delete UserOp
test_delete_user_op_success(_Config) ->
  %% Create a UserOp
  Params = #domain_conf_v2_UserOpParams{
    email = <<"deleteuser@example.com">>,
    name = <<"Delete User">>
  },
  {ok, #domain_conf_v2_UserOp{id = ID, email = <<"deleteuser@example.com">>, name = <<"Delete User">>}} =
    dmt_v2_user_op_handler:create(Params),

  %% Delete the UserOp
  ok = dmt_v2_user_op_handler:delete(ID),

  %% Attempt to get the deleted UserOp
  {error, #domain_conf_v2_UserOpNotFound{}} =
    dmt_v2_user_op_handler:get(ID).

%% Test Delete UserOp when not found
test_delete_user_op_not_found(_Config) ->
  Ref = <<"nonexistent_user_op">>,
  case dmt_v2_user_op_handler:delete(Ref) of
    {error, #domain_conf_v2_UserOpNotFound{}} ->
      ok;
    Other ->
      ?fail("Expected UserOpNotFound on delete, got: ~p", [Other])
  end.