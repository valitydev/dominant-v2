-module(dmt_repository_client_tests_SUITE).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("damsel/include/dmsl_domain_thrift.hrl").
-include_lib("stdlib/include/assert.hrl").

%% API
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2,
    all/0,
    groups/0
]).

-export([
    checkout_object_test/1,
    checkout_objects_test/1,
    checkout_objects_empty_result_test/1,
    checkout_objects_partial_result_test/1
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
        {group, tests}
    ].

%% Define test groups
groups() ->
    [
        {tests, [], [
            checkout_object_test,
            checkout_objects_test,
            checkout_objects_empty_result_test,
            checkout_objects_partial_result_test
        ]}
    ].

init_per_group(_, C) ->
    C.

end_per_group(_, _C) ->
    ok.

init_per_testcase(_, C) ->
    AuthorID = create_author(<<"checkout_objects_test@tests">>, dmt_ct_helper:cfg(client, C)),
    [{author_id, AuthorID} | C].

end_per_testcase(_, _C) ->
    dmt_ct_helper:cleanup_db(),
    ok.

%% Test Cases

checkout_object_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    AuthorID = dmt_ct_helper:cfg(author_id, Config),

    % Create a test object data
    CategoryData = #domain_Category{
        name = <<"Test Checkout Category">>,
        description = <<"Test Checkout Category Description">>
    },

    InsertOp = #domain_conf_v2_InsertOp{
        object = {category, CategoryData}
    },

    % Insert the object
    {ok, #domain_conf_v2_CommitResponse{
        version = Version,
        new_objects = NewObjectsCommit
    }} = dmt_client:commit(0, [{insert, InsertOp}], AuthorID, Client),

    % Extract the created category's ID (binary string)
    % NewObjectsCommit is an ordset of {Type, SpecificObject}, e.g. {category, #domain_CategoryObject{...}}
    % We expect a single object of type category here.
    [{category, CreatedCategoryDomainObject}] = ordsets:to_list(NewObjectsCommit),
    % This is the binary ID
    CreatedCategoryId = CreatedCategoryDomainObject#domain_CategoryObject.ref,

    % Construct the full object reference for checkout {Type, ID}
    ObjectRefForCheckout = {category, CreatedCategoryId},

    % Checkout the object by version
    {ok, VersionObject} = dmt_client:checkout_object(
        {version, Version}, ObjectRefForCheckout, Client
    ),
    #domain_conf_v2_VersionedObject{
        % VersionCategoryDetails is #domain_CategoryObject
        object = {category, VersionCategoryDetails}
    } = VersionObject,
    ReturnedCategoryIdFromVersion = VersionCategoryDetails#domain_CategoryObject.ref,
    ?assertEqual(CreatedCategoryId, ReturnedCategoryIdFromVersion),

    % Checkout by head
    {ok, HeadObject} = dmt_client:checkout_object(
        {head, #domain_conf_v2_Head{}}, ObjectRefForCheckout, Client
    ),
    #domain_conf_v2_VersionedObject{
        % HeadCategoryDetails is #domain_CategoryObject
        object = {category, HeadCategoryDetails}
    } = HeadObject,
    ReturnedCategoryIdFromHead = HeadCategoryDetails#domain_CategoryObject.ref,
    ?assertEqual(CreatedCategoryId, ReturnedCategoryIdFromHead).

checkout_objects_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    AuthorID = dmt_ct_helper:cfg(author_id, Config),

    % Create multiple test object data
    Category1Data = #domain_Category{
        name = <<"Test Category 1">>,
        description = <<"Test Category Description 1">>
    },
    Category2Data = #domain_Category{
        name = <<"Test Category 2">>,
        description = <<"Test Category Description 2">>
    },

    InsertOps = [
        {insert, #domain_conf_v2_InsertOp{object = {category, Category1Data}}},
        {insert, #domain_conf_v2_InsertOp{object = {category, Category2Data}}}
    ],

    % Insert the objects
    {ok, #domain_conf_v2_CommitResponse{
        version = Version,
        new_objects = NewObjectsCommit
    }} = dmt_client:commit(0, InsertOps, AuthorID, Client),

    % Extract the {Type, ID} references for the created objects
    % NewObjectsCommit is an ordset of {Type, SpecificObject}, order maintained by ordsets:to_list/1 if underlying IDs are sortable/consistent.
    CreatedObjectRefsForCheckout = lists:map(
        fun({category, CategoryDomainObject}) ->
            % CategoryDomainObject is #domain_CategoryObject

            % This is the binary ID
            CategoryId = CategoryDomainObject#domain_CategoryObject.ref,
            % Construct {Type, ID} tuple
            {category, CategoryId}
        end,
        % Convert ordset to list
        ordsets:to_list(NewObjectsCommit)
    ),

    % Ensure we have refs to checkout
    ?assertEqual(2, length(CreatedObjectRefsForCheckout)),

    % Checkout multiple objects by version
    {ok, VersionObjects} = dmt_client:checkout_objects(
        {version, Version}, CreatedObjectRefsForCheckout, Client
    ),
    ?assertEqual(2, length(VersionObjects)),

    % Checkout by head
    {ok, HeadObjects} = dmt_client:checkout_objects(
        {head, #domain_conf_v2_Head{}}, CreatedObjectRefsForCheckout, Client
    ),
    ?assertEqual(2, length(HeadObjects)),

    % Verify object IDs were preserved in order from the request
    % ExpectedObjectIds should be the list of binary IDs, in the order they were requested (and hopefully returned by commit)

    % Extracts the ID part
    ExpectedObjectIds = [element(2, RefTuple) || RefTuple <- CreatedObjectRefsForCheckout],

    ActualObjectIds = lists:map(
        fun(VersionedObject) ->
            #domain_conf_v2_VersionedObject{object = {category, CategoryDetails}} = VersionedObject,
            % CategoryDetails is #domain_CategoryObject

            % This is the binary ID
            CategoryDetails#domain_CategoryObject.ref
        end,
        HeadObjects
    ),
    ?assertEqual(ExpectedObjectIds, ActualObjectIds).

checkout_objects_empty_result_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Try to checkout objects that don't exist
    NonExistentRefs = [
        {category, <<"non_existent_category1">>},
        {category, <<"non_existent_category2">>}
    ],

    {error, _Reason} = dmt_client:checkout_objects(
        {head, #domain_conf_v2_Head{}}, NonExistentRefs, Client
    ).

checkout_objects_partial_result_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    AuthorID = dmt_ct_helper:cfg(author_id, Config),

    % Create a single test object data
    CategoryData = #domain_Category{
        name = <<"Partial Test Category">>,
        description = <<"Partial Test Category Description">>
    },

    InsertOp = #domain_conf_v2_InsertOp{
        object = {category, CategoryData}
    },

    % Insert the object
    {ok, #domain_conf_v2_CommitResponse{
        version = Version,
        new_objects = NewObjectsCommit
    }} = dmt_client:commit(0, [{insert, InsertOp}], AuthorID, Client),

    % Extract the created category's ID (binary string)
    [{category, CreatedCategoryDomainObject}] = ordsets:to_list(NewObjectsCommit),
    % This is the binary ID
    CreatedCategoryId = CreatedCategoryDomainObject#domain_CategoryObject.ref,

    % Try to checkout this object and a non-existent one
    MixedRefs = [
        % Use the {Type, ID} of the actual created object
        {category, CreatedCategoryId},
        % A non-existent one
        {category, <<"non_existent_category_id">>}
    ],

    % Get objects by version reference
    {error, _Reason} = dmt_client:checkout_objects({version, Version}, MixedRefs, Client).

% Helper functions

create_author(Email, Client) ->
    AuthorParams = #domain_conf_v2_AuthorParams{
        email = Email,
        name = <<"some_name">>
    },

    {ok, #domain_conf_v2_Author{
        id = AuthorID
    }} = dmt_client:create_author(AuthorParams, Client),
    AuthorID.
