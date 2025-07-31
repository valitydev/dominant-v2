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
    checkout_objects_partial_result_test/1,
    checkout_snapshot_test/1,
    checkout_snapshot_head_test/1,
    checkout_snapshot_non_existent_version_test/1
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
            checkout_objects_partial_result_test,
            checkout_snapshot_test,
            checkout_snapshot_head_test,
            checkout_snapshot_non_existent_version_test
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
    % NewObjectsCommit is an ordset of {Type, SpecificObject},
    % order maintained by ordsets:to_list/1 if underlying IDs are sortable/consistent.
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
    % ExpectedObjectIds should be the list of binary IDs,
    % in the order they were requested (and hopefully returned by commit)

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

checkout_snapshot_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    AuthorID = dmt_ct_helper:cfg(author_id, Config),

    % Create a test category object
    Category1Data = #domain_Category{
        name = <<"Snapshot Test Category 1">>,
        description = <<"Snapshot Test Category Description 1">>
    },
    InsertOp1 = #domain_conf_v2_InsertOp{object = {category, Category1Data}},

    % Create a test term set hierarchy object
    TermSetHierarchyData = #domain_TermSetHierarchy{
        name = <<"Snapshot Test TermSet">>,
        description = <<"Snapshot Test TermSet Description">>,
        term_set = #domain_TermSet{}
    },
    InsertOp2 = #domain_conf_v2_InsertOp{object = {term_set_hierarchy, TermSetHierarchyData}},

    % Insert the objects
    {ok, #domain_conf_v2_CommitResponse{
        version = Version,
        new_objects = NewObjectsCommit
    }} = dmt_client:commit(0, [{insert, InsertOp1}, {insert, InsertOp2}], AuthorID, Client),
    ?assertEqual(2, ordsets:size(NewObjectsCommit)),

    % Checkout snapshot by version
    {ok, #domain_conf_v2_Snapshot{
        version = SnapshotVersion,
        domain = Domain,
        changed_by = Author
    }} = dmt_client:checkout_snapshot({version, Version}, Client),

    ?assertEqual(Version, SnapshotVersion),
    #domain_conf_v2_Author{id = SnapshotAuthorID} = Author,
    ?assertEqual(AuthorID, SnapshotAuthorID),
    ?assertEqual(2, maps:size(Domain)),

    CreatedObjectsList = ordsets:to_list(NewObjectsCommit),

    {category, CreatedCategoryObject} = lists:keyfind(category, 1, CreatedObjectsList),
    #domain_CategoryObject{ref = CategoryRefID} = CreatedCategoryObject,
    ?assert(maps:is_key({category, CategoryRefID}, Domain)),
    {category, SnapCategoryDataRecord} = maps:get({category, CategoryRefID}, Domain),

    #domain_CategoryObject{
        data = #domain_Category{name = SnapCategoryName}
    } = SnapCategoryDataRecord,
    #domain_Category{name = InitialCategoryName} = Category1Data,
    ?assertEqual(InitialCategoryName, SnapCategoryName),

    {term_set_hierarchy, CreatedTermSetObject} = lists:keyfind(
        term_set_hierarchy, 1, CreatedObjectsList
    ),
    #domain_TermSetHierarchyObject{ref = TermSetRefID} = CreatedTermSetObject,
    ?assert(maps:is_key({term_set_hierarchy, TermSetRefID}, Domain)),
    {term_set_hierarchy, SnapTermSetDataRecord} = maps:get(
        {term_set_hierarchy, TermSetRefID}, Domain
    ),

    #domain_TermSetHierarchyObject{
        data = #domain_TermSetHierarchy{name = SnapTermSetName}
    } = SnapTermSetDataRecord,
    #domain_TermSetHierarchy{name = InitialTermSetName} = TermSetHierarchyData,
    ?assertEqual(InitialTermSetName, SnapTermSetName).

checkout_snapshot_head_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    AuthorID = dmt_ct_helper:cfg(author_id, Config),

    %% --- Commit 1: Initial objects ---
    Category1Data = #domain_Category{
        name = <<"Head Test Category 1">>,
        description = <<"Initial version">>
    },
    TermSet1Data = #domain_TermSetHierarchy{
        name = <<"Head Test TermSet 1">>,
        description = <<"Initial version">>,
        term_set = #domain_TermSet{}
    },
    InsertOpCategory1 = {insert, #domain_conf_v2_InsertOp{object = {category, Category1Data}}},
    InsertOpTermSet1 =
        {insert, #domain_conf_v2_InsertOp{object = {term_set_hierarchy, TermSet1Data}}},

    {ok, #domain_conf_v2_CommitResponse{
        version = Version1,
        new_objects = NewObjectsVersion1
    }} = dmt_client:commit(0, [InsertOpCategory1, InsertOpTermSet1], AuthorID, Client),
    ?assertEqual(2, ordsets:size(NewObjectsVersion1)),

    % Extract IDs from Commit 1
    Version1ObjectsList = ordsets:to_list(NewObjectsVersion1),
    {category, #domain_CategoryObject{ref = Cat1RefID}} = lists:keyfind(
        category, 1, Version1ObjectsList
    ),
    {term_set_hierarchy, #domain_TermSetHierarchyObject{ref = TermSet1RefID}} = lists:keyfind(
        term_set_hierarchy, 1, Version1ObjectsList
    ),

    % Checkout head after Commit 1 (optional check, can be removed if too verbose)
    {ok, #domain_conf_v2_Snapshot{
        version = Snapshot1Version,
        domain = Snapshot1Domain
    }} = dmt_client:checkout_snapshot({head, #domain_conf_v2_Head{}}, Client),
    ?assertEqual(Version1, Snapshot1Version),
    ?assertEqual(2, maps:size(Snapshot1Domain)),

    %% --- Commit 2: Update Cat1 and Insert Cat2 (referencing Version1) ---
    UpdatedCategory1Data = Category1Data#domain_Category{description = <<"Updated in V2">>},
    UpdateOpCategory1 =
        {update, #domain_conf_v2_UpdateOp{
            object =
                {category, #domain_CategoryObject{
                    ref = Cat1RefID,
                    data = UpdatedCategory1Data
                }}
        }},
    Category2Data = #domain_Category{
        name = <<"Head Test Category 2">>,
        description = <<"Added in V2">>
    },
    InsertOpCategory2 = {insert, #domain_conf_v2_InsertOp{object = {category, Category2Data}}},

    {ok, #domain_conf_v2_CommitResponse{
        version = Version2,
        % Will contain Cat2, but not necessarily the updated Cat1
        new_objects = NewObjectsVersion2
    }} = dmt_client:commit(Version1, [UpdateOpCategory1, InsertOpCategory2], AuthorID, Client),

    % Extract ID for Cat2 if present in new_objects (it should be)
    Cat2RefID =
        case ordsets:to_list(NewObjectsVersion2) of
            [{category, #domain_CategoryObject{ref = RefCat2}}] ->
                RefCat2;
            Value ->
                ct:fail(#{
                    reason => "Category2 not found in NewObjectsVersion2 as expected",
                    value => Value
                })
        end,

    % Checkout head after Commit 2
    {ok, #domain_conf_v2_Snapshot{
        version = Snapshot2Version,
        domain = Snapshot2Domain,
        changed_by = Snapshot2Author
    }} = dmt_client:checkout_snapshot({head, #domain_conf_v2_Head{}}, Client),

    ?assertEqual(Version2, Snapshot2Version),
    #domain_conf_v2_Author{id = Snapshot2AuthorID} = Snapshot2Author,
    ?assertEqual(AuthorID, Snapshot2AuthorID),
    % Cat1 (updated), TermSet1 (original), Cat2 (new)
    ?assertEqual(3, maps:size(Snapshot2Domain)),

    % Check updated Cat1
    {category, #domain_CategoryObject{
        data = #domain_Category{
            description = Snapshot2Category1Description, name = Snapshot2Category1Name
        }
    }} = maps:get({category, Cat1RefID}, Snapshot2Domain),
    ?assertEqual(UpdatedCategory1Data#domain_Category.description, Snapshot2Category1Description),
    % Name should be original
    ?assertEqual(Category1Data#domain_Category.name, Snapshot2Category1Name),

    % Check TermSet1 (should be unchanged from Version1)
    {term_set_hierarchy, #domain_TermSetHierarchyObject{
        data = #domain_TermSetHierarchy{
            name = Snapshot2TermSet1Name, description = Snapshot2TermSet1Description
        }
    }} = maps:get({term_set_hierarchy, TermSet1RefID}, Snapshot2Domain),
    ?assertEqual(TermSet1Data#domain_TermSetHierarchy.name, Snapshot2TermSet1Name),
    ?assertEqual(TermSet1Data#domain_TermSetHierarchy.description, Snapshot2TermSet1Description),

    % Check new Cat2
    {category, #domain_CategoryObject{
        data = #domain_Category{
            name = Snapshot2Category2Name, description = Snapshot2Category2Description
        }
    }} = maps:get({category, Cat2RefID}, Snapshot2Domain),
    ?assertEqual(Category2Data#domain_Category.name, Snapshot2Category2Name),
    ?assertEqual(Category2Data#domain_Category.description, Snapshot2Category2Description).

checkout_snapshot_non_existent_version_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    {exception, #domain_conf_v2_VersionNotFound{}} = dmt_client:checkout_snapshot(
        {version, 999999}, Client
    ).

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
