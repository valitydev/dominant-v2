-module(dmt_integration_tests_SUITE).

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
    %%    AuthorManagement Tests
    create_author_test/1,
    get_author_test/1,
    delete_author_test/1,
    delete_nonexistant_author_test/1,
    create_author_email_duplicate_test/1,
    get_author_by_email_test/1
]).

-export([
    %% Repository Tests
    insert_object_forced_id_success_test/1,
    insert_object_sequence_id_success_test/1,
    insert_remove_referencing_object_success_test/1,
    update_object_success_test/1,
    get_latest_version_test/1,
    get_all_objects_history_test/1,
    get_all_objects_history_pagination_test/1,
    get_object_history_test/1,
    get_object_history_pagination_test/1,
    get_object_history_nonexistent_test/1
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
        {group, author_tests},
        {group, repository_tests}
    ].

%% Define test groups
groups() ->
    [
        {author_tests, [], [
            create_author_test,
            get_author_test,
            delete_author_test,
            delete_nonexistant_author_test,
            create_author_email_duplicate_test,
            get_author_by_email_test
        ]},
        {repository_tests, [], [
            insert_object_forced_id_success_test,
            insert_object_sequence_id_success_test,
            insert_remove_referencing_object_success_test,
            update_object_success_test,
            get_latest_version_test,
            get_all_objects_history_test,
            get_all_objects_history_pagination_test,
            get_object_history_test,
            get_object_history_pagination_test,
            get_object_history_nonexistent_test
        ]}
    ].

init_per_group(_, C) ->
    C.

end_per_group(_, _C) ->
    ok.

end_per_testcase(_, _C) ->
    dmt_ct_helper:cleanup_db(),
    ok.

%% Test Cases

%% AuthorManagement Tests

create_author_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    AuthorParams = #domain_conf_v2_AuthorParams{
        email = <<"create_author_test@test">>,
        name = <<"some_name">>
    },

    {ok, _} = dmt_client:create_author(AuthorParams, Client).

get_author_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    Email = <<"get_author_test">>,
    AuthorID = create_author(Email, Client),
    {ok, #domain_conf_v2_Author{
        email = Email1
    }} = dmt_client:get_author(AuthorID, Client),
    ?assertEqual(Email, Email1).

delete_author_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    Email = <<"delete_author_test">>,
    AuthorID = create_author(Email, Client),
    {ok, ok} = dmt_client:delete_author(AuthorID, Client),
    {exception, #domain_conf_v2_AuthorNotFound{}} =
        dmt_client:get_author(AuthorID, Client).

delete_nonexistant_author_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % some string
    {exception, #domain_conf_v2_AuthorNotFound{}} =
        dmt_client:delete_author(<<"nonexistant_id">>, Client),
    % unknown uuid
    {exception, #domain_conf_v2_AuthorNotFound{}} =
        dmt_client:delete_author(uuid:get_v4(), Client).

create_author_email_duplicate_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    AuthorParams1 = #domain_conf_v2_AuthorParams{
        email = <<"create_author_email_duplicate_test@test">>,
        name = <<"some_name">>
    },

    AuthorParams2 = #domain_conf_v2_AuthorParams{
        email = <<"create_author_email_duplicate_test@test">>,
        name = <<"different name">>
    },

    {ok, #domain_conf_v2_Author{
        id = ID1
    }} = dmt_client:create_author(AuthorParams1, Client),
    {exception, #domain_conf_v2_AuthorAlreadyExists{id = ID2}} =
        dmt_client:create_author(AuthorParams2, Client),
    ?assertEqual(ID1, ID2).

%% Test getting an author by email
get_author_by_email_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    Email = <<"get_author_by_email_test@test">>,
    Name = <<"Get By Email Test Author">>,

    AuthorParams = #domain_conf_v2_AuthorParams{
        email = Email,
        name = Name
    },

    {ok, #domain_conf_v2_Author{
        id = AuthorID,
        email = Email
    }} = dmt_client:create_author(AuthorParams, Client),

    % Get author by email - retrying to allow for potential database propagation delay
    {ok, #domain_conf_v2_Author{
        id = EmailAuthorID,
        email = EmailResult,
        name = NameResult
    }} = dmt_client:get_author_by_email(Email, Client),

    % Verify the result matches the original author
    ?assertEqual(AuthorID, EmailAuthorID, "Author ID should match"),
    ?assertEqual(Email, EmailResult, "Email should match"),
    ?assertEqual(Name, NameResult, "Name should match"),

    % Test with non-existent email
    NonExistentEmail = <<"nonexistent_email@test">>,
    {exception, #domain_conf_v2_AuthorNotFound{}} =
        dmt_client:get_author_by_email(NonExistentEmail, Client).

%% Repository tests

insert_remove_referencing_object_success_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"insert_remove_referencing_object_success_test">>,
    AuthorID = create_author(Email, Client),

    Revision1 = 0,
    Operations1 = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {proxy, #domain_ProxyDefinition{
                    name = <<"proxy">>,
                    description = <<"proxy_description">>,
                    url = <<"http://someurl">>,
                    options = #{}
                }}
        }}
    ],
    {ok, #domain_conf_v2_CommitResponse{
        version = Revision2,
        new_objects = [
            {proxy, #domain_ProxyObject{
                ref = ProxyRef
            }}
        ]
    }} = dmt_client:commit(Revision1, Operations1, AuthorID, Client),

    Operations2 = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {provider, #domain_Provider{
                    name = <<"name">>,
                    realm = test,
                    description = <<"description">>,
                    proxy = #domain_Proxy{
                        ref = ProxyRef,
                        additional = #{}
                    }
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision3,
        new_objects = [
            {provider, #domain_ProviderObject{
                ref = _ProviderRef
            }}
        ]
    }} = dmt_client:commit(Revision2, Operations2, AuthorID, Client),

    %%  try to remove proxy
    Operations3 = [
        {remove, #domain_conf_v2_RemoveOp{
            ref = {proxy, ProxyRef}
        }}
    ],

    {ok, _} = dmt_client:commit(Revision3, Operations3, AuthorID, Client).

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
%%    }} = dmt_client:commit(Revision3, Commit4, AuthorID, Client),
%%
%%%%  try to remove proxy again
%%    {ok, #domain_conf_v2_CommitResponse{}} =
%%        dmt_client:commit(Revision4, Commit3, AuthorID, Client).

insert_object_sequence_id_success_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"insert_object_sequence_id_success_test">>,
    AuthorID = create_author(Email, Client),

    %% Insert a test object
    Revision = 0,
    Category = #domain_Category{
        name = <<"name1">>,
        description = <<"description1">>
    },
    Operations = [
        {insert, #domain_conf_v2_InsertOp{
            object = {category, Category}
        }},
        {insert, #domain_conf_v2_InsertOp{
            object = {category, Category}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        new_objects = [
            {category, #domain_CategoryObject{
                ref = #domain_CategoryRef{id = ID1}
            }},
            {category, #domain_CategoryObject{
                ref = #domain_CategoryRef{id = ID2}
            }}
        ]
    }} = dmt_client:commit(Revision, Operations, AuthorID, Client),

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
    AuthorID = create_author(Email, Client),

    %% Insert a test object
    Revision = 0,
    CategoryRef = #domain_CategoryRef{id = 1337},
    ForcedRef = {category, CategoryRef},
    Category = #domain_Category{
        name = <<"name">>,
        description = <<"description">>
    },
    Operations = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, Category},
            force_ref = ForcedRef
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        new_objects = NewObjectsSet
    }} = dmt_client:commit(Revision, Operations, AuthorID, Client),

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
    AuthorID = create_author(Email, Client),

    Revision1 = 0,
    Operations1 = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {proxy, #domain_ProxyDefinition{
                    name = <<"proxy">>,
                    description = <<"proxy_description">>,
                    url = <<"http://someurl">>,
                    options = #{}
                }}
        }}
    ],
    {ok, #domain_conf_v2_CommitResponse{
        version = Revision2,
        new_objects = [
            {proxy, #domain_ProxyObject{
                ref = ProxyRef
            }}
        ]
    }} = dmt_client:commit(Revision1, Operations1, AuthorID, Client),

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

    Operations2 = [
        {update, #domain_conf_v2_UpdateOp{
            object = NewObject
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision3
    }} = dmt_client:commit(Revision2, Operations2, AuthorID, Client),

    {ok, #domain_conf_v2_VersionedObject{
        object = NewObject
    }} = dmt_client:checkout_object({version, Revision3}, {proxy, ProxyRef}, Client).

get_all_objects_history_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    Email = <<"get_all_objects_history_test">>,
    AuthorID = create_author(Email, Client),

    % Create initial objects and updates to generate history
    Revision0 = 0,

    % First commit - create first category object
    CategoryOps1 = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<"category1">>,
                    description = <<"description1">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision1,
        new_objects = [
            {category, #domain_CategoryObject{
                ref = CategoryRef1
            }}
        ]
    }} = dmt_client:commit(Revision0, CategoryOps1, AuthorID, Client),

    % Second commit - create second category object
    CategoryOps2 = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<"category2">>,
                    description = <<"description2">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision2,
        new_objects = [
            {category, #domain_CategoryObject{
                ref = CategoryRef2
            }}
        ]
    }} = dmt_client:commit(Revision1, CategoryOps2, AuthorID, Client),

    % Third commit - update the first object
    UpdateOps = [
        {update, #domain_conf_v2_UpdateOp{
            object =
                {category, #domain_CategoryObject{
                    ref = CategoryRef1,
                    data = #domain_Category{
                        name = <<"category1-updated">>,
                        description = <<"description1-updated">>
                    }
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision3
    }} = dmt_client:commit(Revision2, UpdateOps, AuthorID, Client),

    % Fetch all object history
    Request = #domain_conf_v2_RequestParams{
        % Get all objects in one request
        limit = 10
    },

    {ok, #domain_conf_v2_ObjectVersionsResponse{
        result = HistoryObjects,
        total_count = Count
    }} = dmt_client:get_all_objects_history(Request, Client),

    % We should have exactly 3 entries for our specific objects
    ?assertEqual(3, Count),

    % Verify all expected objects are in the history with correct versions
    Cat1Update = find_object_in_history(HistoryObjects, {category, CategoryRef1}, Revision3),
    % _ = logger:error("CategoryRef1: ~p, Revision3: ~p", [CategoryRef1, Revision3]),
    Cat2Initial = find_object_in_history(HistoryObjects, {category, CategoryRef2}, Revision2),
    Cat1Initial = find_object_in_history(HistoryObjects, {category, CategoryRef1}, Revision1),

    % _ = logger:error("History objects: ~p", [HistoryObjects]),
    % Verify we found all our expected history entries
    ?assertNotEqual(undefined, Cat1Update, "Updated category1 not found in history"),
    ?assertNotEqual(undefined, Cat2Initial, "Initial category2 not found in history"),
    ?assertNotEqual(undefined, Cat1Initial, "Initial category1 not found in history").

get_all_objects_history_pagination_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    Email = <<"get_all_objects_history_pagination_test">>,
    AuthorID = create_author(Email, Client),

    % Create 5 different category objects
    {_Revision5, CategoryRefs} = create_n_categories(5, 0, AuthorID, Client, []),

    % First page with small limit to force pagination
    Request1 = #domain_conf_v2_RequestParams{
        limit = 2
    },

    {ok, #domain_conf_v2_ObjectVersionsResponse{
        result = FirstPageObjects,
        total_count = FirstPageCount,
        continuation_token = ContinuationToken1
    }} = dmt_client:get_all_objects_history(Request1, Client),

    % Verify we got the expected number of objects in first page
    ?assertEqual(2, length(FirstPageObjects)),
    ?assertEqual(2, FirstPageCount),

    % Verify continuation token exists (we should have more pages)
    ?assertNotEqual(undefined, ContinuationToken1, "Expected continuation token for pagination"),

    % Get second page
    Request2 = #domain_conf_v2_RequestParams{
        limit = 2,
        continuation_token = ContinuationToken1
    },

    {ok, #domain_conf_v2_ObjectVersionsResponse{
        result = SecondPageObjects,
        total_count = SecondPageCount,
        continuation_token = ContinuationToken2
    }} = dmt_client:get_all_objects_history(Request2, Client),

    % Verify second page results
    ?assertEqual(2, length(SecondPageObjects)),
    ?assertEqual(2, SecondPageCount),

    % Verify we have another page
    ?assertNotEqual(undefined, ContinuationToken2, "Expected continuation token for third page"),

    % Get third (final) page
    Request3 = #domain_conf_v2_RequestParams{
        limit = 2,
        continuation_token = ContinuationToken2
    },

    {ok, #domain_conf_v2_ObjectVersionsResponse{
        result = ThirdPageObjects,
        total_count = ThirdPageCount,
        continuation_token = FinalContinuationToken
    }} = dmt_client:get_all_objects_history(Request3, Client),

    % Verify final page results - since we have 5 total objects, the last page should have 1
    ?assertEqual(1, length(ThirdPageObjects)),
    ?assertEqual(1, ThirdPageCount),

    % Verify this is the last page (no more continuation token)
    ?assertEqual(undefined, FinalContinuationToken, "Expected no more pages"),

    % Verify we got all 5 objects across all pages
    TotalObjects = FirstPageObjects ++ SecondPageObjects ++ ThirdPageObjects,
    ?assertEqual(5, length(TotalObjects)),

    % Check that our objects are ordered by version in descending order
    VersionsInOrder = [
        V
     || #domain_conf_v2_LimitedVersionedObject{
            info = #domain_conf_v2_VersionedObjectInfo{version = V}
        } <- TotalObjects
    ],
    ?assert(
        is_sorted_desc(VersionsInOrder), "Objects should be sorted by version in descending order"
    ),

    % Verify all our category refs are included in the history
    AllCategoryRefsFound = lists:all(
        fun(CategoryRef) ->
            % Extract just the ID number for comparison
            RefId = element(2, CategoryRef),
            % Check if any object in TotalObjects has this category ID
            lists:any(
                fun(
                    #domain_conf_v2_LimitedVersionedObject{
                        ref = {category, #domain_CategoryRef{id = ObjId}}
                    }
                ) ->
                    RefId =:= ObjId
                end,
                TotalObjects
            )
        end,
        CategoryRefs
    ),
    ?assert(AllCategoryRefsFound, "Not all created categories found in history pages").

get_latest_version_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    Email = <<"get_latest_version_test">>,
    AuthorID = create_author(Email, Client),

    {ok, Revision0} = dmt_client:get_latest_version(Client),
    ?assertEqual(0, Revision0),

    Operations = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<"name">>,
                    description = <<"description">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision1
    }} = dmt_client:commit(Revision0, Operations, AuthorID, Client),

    {ok, Revision2} = dmt_client:get_latest_version(Client),
    ?assertEqual(Revision2, Revision1),
    ?assertEqual(1, Revision1).

%% Test retrieval of a single object's history
get_object_history_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    Email = <<"get_object_history_test@test">>,
    AuthorID = create_author(Email, Client),

    % Create initial object
    Revision0 = 0,
    CategoryName1 = <<"history_category">>,
    CategoryDesc1 = <<"original description">>,

    % First commit - create the object
    CategoryOps1 = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = CategoryName1,
                    description = CategoryDesc1
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision1,
        new_objects = [
            {category, #domain_CategoryObject{
                ref = CategoryRef
            }}
        ]
    }} = dmt_client:commit(Revision0, CategoryOps1, AuthorID, Client),

    % Second commit - update the object
    CategoryName2 = <<"history_category_updated">>,
    CategoryDesc2 = <<"updated description">>,
    UpdateOps = [
        {update, #domain_conf_v2_UpdateOp{
            object =
                {category, #domain_CategoryObject{
                    ref = CategoryRef,
                    data = #domain_Category{
                        name = CategoryName2,
                        description = CategoryDesc2
                    }
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision2
    }} = dmt_client:commit(Revision1, UpdateOps, AuthorID, Client),

    % Third commit - another update
    CategoryName3 = <<"history_category_final">>,
    CategoryDesc3 = <<"final description">>,
    UpdateOps2 = [
        {update, #domain_conf_v2_UpdateOp{
            object =
                {category, #domain_CategoryObject{
                    ref = CategoryRef,
                    data = #domain_Category{
                        name = CategoryName3,
                        description = CategoryDesc3
                    }
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision3
    }} = dmt_client:commit(Revision2, UpdateOps2, AuthorID, Client),

    % Get history for the object
    ObjectRef = {category, CategoryRef},
    RequestParams = #domain_conf_v2_RequestParams{
        limit = 10
    },

    {ok, #domain_conf_v2_ObjectVersionsResponse{
        result = HistoryObjects,
        total_count = Count,
        continuation_token = ContinuationToken
    }} = dmt_client:get_object_history(ObjectRef, RequestParams, Client),

    % We should have exactly 3 versions
    ?assertEqual(3, Count),
    ?assertEqual(undefined, ContinuationToken, "No continuation token expected"),
    ?assertEqual(3, length(HistoryObjects), "Should return all 3 versions"),

    % Verify objects are in correct order (newest first)
    [Version1, Version2, Version3] = HistoryObjects,

    #domain_conf_v2_LimitedVersionedObject{info = #domain_conf_v2_VersionedObjectInfo{version = V1}} =
        Version1,
    #domain_conf_v2_LimitedVersionedObject{info = #domain_conf_v2_VersionedObjectInfo{version = V2}} =
        Version2,
    #domain_conf_v2_LimitedVersionedObject{info = #domain_conf_v2_VersionedObjectInfo{version = V3}} =
        Version3,

    ?assertEqual(Revision3, V1, "Latest version should be first"),
    ?assertEqual(Revision2, V2, "Middle version should be second"),
    ?assertEqual(Revision1, V3, "First version should be last"),

    % Verify all objects have correct reference
    #domain_conf_v2_LimitedVersionedObject{ref = R1} =
        Version1,
    #domain_conf_v2_LimitedVersionedObject{ref = R2} =
        Version2,
    #domain_conf_v2_LimitedVersionedObject{ref = R3} =
        Version3,

    ?assertEqual(ObjectRef, R1),
    ?assertEqual(ObjectRef, R2),
    ?assertEqual(ObjectRef, R3).

%% Test pagination functionality for object history
get_object_history_pagination_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    Email = <<"get_object_history_pagination_test@test">>,
    AuthorID = create_author(Email, Client),

    % Create an object and perform multiple updates to generate history
    Revision0 = 0,
    CategoryRef = create_category_with_updates(5, Revision0, AuthorID, Client),
    ObjectRef = {category, CategoryRef},

    % First page with small limit to force pagination
    Request1 = #domain_conf_v2_RequestParams{
        limit = 2
    },

    {ok, #domain_conf_v2_ObjectVersionsResponse{
        result = FirstPageObjects,
        total_count = FirstPageCount,
        continuation_token = ContinuationToken1
    }} = dmt_client:get_object_history(ObjectRef, Request1, Client),

    % Verify first page results
    ?assertEqual(2, FirstPageCount, "First page should have exactly 2 results"),
    ?assertEqual(2, length(FirstPageObjects), "First page should contain 2 objects"),
    ?assertNotEqual(undefined, ContinuationToken1, "Should have continuation token for next page"),

    % Get second page
    Request2 = #domain_conf_v2_RequestParams{
        limit = 2,
        continuation_token = ContinuationToken1
    },

    {ok, #domain_conf_v2_ObjectVersionsResponse{
        result = SecondPageObjects,
        total_count = SecondPageCount,
        continuation_token = ContinuationToken2
    }} = dmt_client:get_object_history(ObjectRef, Request2, Client),

    % Verify second page results
    ?assertEqual(2, SecondPageCount, "Second page should have exactly 2 results"),
    ?assertEqual(2, length(SecondPageObjects), "Second page should contain 2 objects"),
    ?assertNotEqual(undefined, ContinuationToken2, "Should have continuation token for final page"),

    % Get third page (final)
    Request3 = #domain_conf_v2_RequestParams{
        limit = 2,
        continuation_token = ContinuationToken2
    },

    {ok, #domain_conf_v2_ObjectVersionsResponse{
        result = ThirdPageObjects,
        total_count = ThirdPageCount,
        continuation_token = FinalToken
    }} = dmt_client:get_object_history(ObjectRef, Request3, Client),

    % Verify final page results - should have just 1 remaining object
    ?assertEqual(1, ThirdPageCount, "Third page should have exactly 1 result"),
    ?assertEqual(1, length(ThirdPageObjects), "Third page should contain 1 object"),
    ?assertEqual(undefined, FinalToken, "No more pagination expected"),

    % Verify we got all 5 objects across all pages with correct order (newest first)
    AllObjects = FirstPageObjects ++ SecondPageObjects ++ ThirdPageObjects,
    ?assertEqual(5, length(AllObjects), "Should get 5 objects across all pages"),

    % Check versions are in descending order
    Versions = [
        V
     || #domain_conf_v2_LimitedVersionedObject{
            info = #domain_conf_v2_VersionedObjectInfo{version = V}
        } <- AllObjects
    ],
    ?assert(is_sorted_desc(Versions), "Versions should be in descending order").

%% Test behavior for nonexistent object reference
get_object_history_nonexistent_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Create a reference to a nonexistent object
    NonexistentCategoryRef = #domain_CategoryRef{id = 999999},
    NonexistentRef = {category, NonexistentCategoryRef},

    RequestParams = #domain_conf_v2_RequestParams{
        limit = 10
    },

    % Expect object not found error for nonexistent object
    Result = dmt_client:get_object_history(NonexistentRef, RequestParams, Client),
    ?assertMatch({exception, #domain_conf_v2_ObjectNotFound{}}, Result).

% Internal functions

%% Helper function to create a category and make multiple updates to it
create_category_with_updates(NumUpdates, InitialRevision, AuthorID, Client) ->
    % Create initial category
    CategoryOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<"versioned_category">>,
                    description = <<"initial version">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision1,
        new_objects = [
            {category, #domain_CategoryObject{
                ref = CategoryRef
            }}
        ]
    }} = dmt_client:commit(InitialRevision, CategoryOps, AuthorID, Client),

    % Perform multiple updates
    lists:foldl(
        fun(N, CurrentRevision) ->
            UpdateDesc = list_to_binary(io_lib:format("update version ~p", [N])),

            UpdateOps = [
                {update, #domain_conf_v2_UpdateOp{
                    object =
                        {category, #domain_CategoryObject{
                            ref = CategoryRef,
                            data = #domain_Category{
                                name = <<"versioned_category">>,
                                description = UpdateDesc
                            }
                        }}
                }}
            ],

            {ok, #domain_conf_v2_CommitResponse{
                version = NextRevision
            }} = dmt_client:commit(CurrentRevision, UpdateOps, AuthorID, Client),

            NextRevision
        end,
        Revision1,
        lists:seq(1, NumUpdates - 1)
    ),

    CategoryRef.

create_author(Email, Client) ->
    AuthorParams = #domain_conf_v2_AuthorParams{
        email = Email,
        name = <<"some_name">>
    },

    {ok, #domain_conf_v2_Author{
        id = AuthorID
    }} = dmt_client:create_author(AuthorParams, Client),
    AuthorID.

create_n_categories(0, CurrentRevision, _AuthorID, _Client, Refs) ->
    {CurrentRevision, lists:reverse(Refs)};
create_n_categories(N, CurrentRevision, AuthorID, Client, Refs) ->
    CategoryName = list_to_binary(io_lib:format("category~p", [N])),
    CategoryOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = CategoryName,
                    description = <<"test category">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = NextRevision,
        new_objects = [
            {category, #domain_CategoryObject{
                ref = CategoryRef
            }}
        ]
    }} = dmt_client:commit(CurrentRevision, CategoryOps, AuthorID, Client),

    create_n_categories(N - 1, NextRevision, AuthorID, Client, [CategoryRef | Refs]).

% Helper to find an object in history by ref and version
find_object_in_history(HistoryObjects, ObjectRef, ExpectedVersion) ->
    lists:foldl(
        fun(
            #domain_conf_v2_LimitedVersionedObject{
                ref = Ref,
                info = #domain_conf_v2_VersionedObjectInfo{version = Version}
            } = Obj,
            Acc
        ) ->
            case {Ref, Version} of
                {ObjectRef, ExpectedVersion} ->
                    Obj;
                _ ->
                    Acc
            end
        end,
        undefined,
        HistoryObjects
    ).

is_sorted_desc([]) ->
    true;
is_sorted_desc([_]) ->
    true;
is_sorted_desc([A, B | Rest]) when A >= B ->
    is_sorted_desc([B | Rest]);
is_sorted_desc(_) ->
    false.
