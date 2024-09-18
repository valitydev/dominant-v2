-module(dmt_v2_integration_test_SUITE).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("damsel/include/dmsl_domain_thrift.hrl").
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
  test_checkout_object_success/1
]).

%% Setup and Teardown

%% Initialize per suite
init_per_suite(Config) ->
  {Apps, _Ret} = dmt_v2_ct_helper:start_apps([woody, scoper, dmt_v2]),
  ApiClient = dmt_v2_ct_helper:create_client(),
  [{client, ApiClient}, {apps, Apps} | Config].

%% Cleanup after suite
end_per_suite(_Config) ->
%%TODO Clean up DB
  ok.

init_per_testcase(_TestCase, _Config) ->
  ok.

end_per_testcase(_TestCase, _Config) ->
%%TODO Clean up DB
  ok.

%% Define all test cases
all() ->
  [
    {group, repository_client_tests}
  ].

%% Define test groups
groups() ->
  [
    {repository_client_tests, [], [
      test_checkout_object_success
    ]}
  ].

%% Test Cases

%% RepositoryClient Tests

%% Test successful CheckoutObject
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
  ForcedRef = {category, #domain_CategoryRef{id = 1}},
  Commit = #domain_conf_v2_Commit{
    ops = [
      #domain_conf_v2_InsertOp{
        object = {category, #domain_Category{
          name = <<"name">>,
          description = <<"description">>
        }},
        force_ref = ForcedRef
      }
    ]
  },

  {ok, NewRevision} = dmt_v2_client:commit(Revision, Commit, UserOpID, Client),

%%  Check that it was added
  {ok} = dmt_v2_client:checkout_object(NewRevision, ForcedRef, Client).
