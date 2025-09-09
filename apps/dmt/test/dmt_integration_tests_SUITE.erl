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
    insert_object_uuid_id_success_test/1,
    insert_remove_referencing_object_success_test/1,
    insert_related_objects_success_test/1,
    update_object_success_test/1,
    commit_author_not_found_test/1,
    get_latest_version_test/1,
    get_all_objects_history_test/1,
    get_all_objects_history_pagination_test/1,
    get_object_history_test/1,
    get_object_history_pagination_test/1,
    get_object_history_nonexistent_test/1,
    delete_referenced_entity_validation_test/1,
    commit_object_with_nonexistent_reference_test/1,
    checkout_object_with_references_test/1,
    get_related_graph_basic_test/1,
    get_related_graph_depth_test/1,
    get_related_graph_inbound_outbound_test/1,
    get_related_graph_type_filter_test/1,
    get_related_graph_complex_chain_test/1,
    get_related_graph_error_handling_test/1,
    remove_referencing_and_referenced_same_commit_success_test/1
]).

-export([
    %% RepositoryClient Tests
]).

%% Setup and Teardown

%% Initialize per suite
init_per_suite(Config) ->
    {Apps, _Ret} = dmt_ct_helper:start_apps([woody, scoper, epg_connector, brod, dmt]),
    ApiClient = dmt_ct_helper:create_client(),
    _ = dmt_ct_helper:create_kafka_topics(),
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
            insert_object_uuid_id_success_test,
            insert_remove_referencing_object_success_test,
            insert_related_objects_success_test,
            update_object_success_test,
            commit_author_not_found_test,
            get_latest_version_test,
            get_all_objects_history_test,
            get_all_objects_history_pagination_test,
            get_object_history_test,
            get_object_history_pagination_test,
            get_object_history_nonexistent_test,
            delete_referenced_entity_validation_test,
            commit_object_with_nonexistent_reference_test,
            checkout_object_with_references_test,
            get_related_graph_basic_test,
            get_related_graph_depth_test,
            get_related_graph_inbound_outbound_test,
            get_related_graph_type_filter_test,
            get_related_graph_complex_chain_test,
            get_related_graph_error_handling_test,
            remove_referencing_and_referenced_same_commit_success_test
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
                ref = ProviderRef
            }}
        ]
    }} = dmt_client:commit(Revision2, Operations2, AuthorID, Client),

    %%  try to remove proxy
    Operations3 = [
        {remove, #domain_conf_v2_RemoveOp{
            ref = {provider, ProviderRef}
        }}
    ],

    {ok, _} = dmt_client:commit(Revision3, Operations3, AuthorID, Client),

    %%  try to remove proxy
    Operations4 = [
        {remove, #domain_conf_v2_RemoveOp{
            ref = {proxy, ProxyRef}
        }}
    ],

    {ok, _} = dmt_client:commit(Revision3, Operations4, AuthorID, Client).

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
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        new_objects = [
            {category, #domain_CategoryObject{
                ref = #domain_CategoryRef{id = ID1}
            }}
        ]
    }} = dmt_client:commit(Revision, Operations, AuthorID, Client),

    {ok, #domain_conf_v2_CommitResponse{
        new_objects = [
            {category, #domain_CategoryObject{
                ref = #domain_CategoryRef{id = ID2}
            }}
        ]
    }} = dmt_client:commit(
        Revision,
        [
            {insert, #domain_conf_v2_InsertOp{
                force_ref = {category, #domain_CategoryRef{id = ID1 + 1}},
                object = {category, Category}
            }}
        ],
        AuthorID,
        Client
    ),

    {ok, #domain_conf_v2_CommitResponse{
        new_objects = [
            {category, #domain_CategoryObject{
                ref = #domain_CategoryRef{id = ID3}
            }}
        ]
    }} = dmt_client:commit(Revision, Operations, AuthorID, Client),

    ?assertMatch(true, is_in_sequence(ID1, ID2)),
    ?assertMatch(true, is_in_sequence(ID2, ID3)).

is_in_sequence(N1, N2) when N1 + 1 =:= N2 ->
    true;
is_in_sequence(N1, N2) when N1 =:= N2 + 1 ->
    true;
is_in_sequence(N1, N2) ->
    {false, N1, N2}.

insert_object_uuid_id_success_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"insert_object_uuid_id_success_test">>,
    AuthorID = create_author(Email, Client),

    %% Insert a test object
    Revision = 0,
    PartyConfig = #domain_PartyConfig{
        name = <<"name1">>,
        block = {unblocked, #domain_Unblocked{reason = <<"reason1">>, since = <<"2025-01-01T00:00:00Z">>}},
        suspension = {active, #domain_Active{since = <<"2025-01-01T00:00:00Z">>}},
        contact_info = #domain_PartyContactInfo{registration_email = <<"test@test.com">>}
    },

    Operations = [
        {insert, #domain_conf_v2_InsertOp{
            object = {party_config, PartyConfig}
        }},
        {insert, #domain_conf_v2_InsertOp{
            object = {party_config, PartyConfig}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        new_objects = [
            {party_config, #domain_PartyConfigObject{
                ref = #domain_PartyConfigRef{id = ID1}
            }},
            {party_config, #domain_PartyConfigObject{
                ref = #domain_PartyConfigRef{id = ID2}
            }}
        ]
    }} = dmt_client:commit(Revision, Operations, AuthorID, Client),

    ?assertEqual(true, uuid:is_uuid(uuid:string_to_uuid(binary_to_list(ID1)))),
    ?assertEqual(true, uuid:is_uuid(uuid:string_to_uuid(binary_to_list(ID2)))),
    ?assertNotEqual(ID1, ID2).

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

insert_related_objects_success_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"insert_object_forced_id_success_test">>,
    AuthorID = create_author(Email, Client),

    %% Insert a test object
    Revision = 0,
    ProxyRef = #domain_ProxyRef{id = 123},
    Operations = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {proxy, #domain_ProxyDefinition{
                    name = <<"basic_test_proxy">>,
                    description = <<"proxy for basic graph test">>,
                    url = <<"http://basic-test-proxy.example.com">>,
                    options = #{}
                }},
            force_ref = {proxy, ProxyRef}
        }},
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

    {ok, #domain_conf_v2_CommitResponse{}} =
        dmt_client:commit(Revision, Operations, AuthorID, Client).

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

commit_author_not_found_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    RandomUUID = uuid:get_v4_urandom(),

    Revision = 0,
    Category = #domain_Category{
        name = <<"name1">>,
        description = <<"description1">>
    },

    Operations = [{insert, #domain_conf_v2_InsertOp{object = {category, Category}}}],

    {exception, #domain_conf_v2_AuthorNotFound{}} = dmt_client:commit(Revision, Operations, <<"">>, Client),
    {exception, #domain_conf_v2_AuthorNotFound{}} = dmt_client:commit(Revision, Operations, RandomUUID, Client).

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

%% Test that deleting an entity with active references fails with validation error
delete_referenced_entity_validation_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"delete_referenced_entity_validation_test">>,
    AuthorID = create_author(Email, Client),

    % Step 1: Create a proxy (target entity that will be referenced)
    Revision1 = 0,
    ProxyOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {proxy, #domain_ProxyDefinition{
                    name = <<"referenced_proxy">>,
                    description = <<"proxy that will be referenced">>,
                    url = <<"http://example.com">>,
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
    }} = dmt_client:commit(Revision1, ProxyOps, AuthorID, Client),

    % Step 2: Create a provider that references the proxy
    ProviderOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {provider, #domain_Provider{
                    name = <<"referencing_provider">>,
                    realm = test,
                    description = <<"provider that references the proxy">>,
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
                ref = ProviderRef
            }}
        ]
    }} = dmt_client:commit(Revision2, ProviderOps, AuthorID, Client),

    % Step 3: Try to remove the referenced proxy - this should FAIL
    RemoveProxyOps = [
        {remove, #domain_conf_v2_RemoveOp{
            ref = {proxy, ProxyRef}
        }}
    ],

    % Expect an operation error due to active references
    {exception, #domain_conf_v2_OperationInvalid{
        errors = [
            {object_not_exists, #domain_conf_v2_NonexistantObject{
                object_ref = {proxy, _},
                referenced_by = ReferencedByList
            }}
        ]
    }} = dmt_client:commit(Revision3, RemoveProxyOps, AuthorID, Client),

    % Verify that the referenced_by list contains the provider ID
    ?assertEqual(1, length(ReferencedByList), "Should have exactly one referencing entity"),
    [ProviderIdString] = ReferencedByList,
    ?assertEqual(
        {provider, ProviderRef},
        ProviderIdString,
        "Referenced by should contain the provider"
    ),

    % Step 4: Remove the provider first (the referencing entity)
    RemoveProviderOps = [
        {remove, #domain_conf_v2_RemoveOp{
            ref = {provider, ProviderRef}
        }}
    ],
    {ok, #domain_conf_v2_CommitResponse{
        version = Revision4
    }} = dmt_client:commit(Revision3, RemoveProviderOps, AuthorID, Client),

    % Step 5: Now try to remove the proxy again - this should FAIL
    {exception, #domain_conf_v2_OperationConflict{
        conflict =
            {object_not_found, #domain_conf_v2_ObjectNotFoundConflict{
                object_ref = {proxy, ProxyRef}
            }}
    }} = dmt_client:commit(Revision4, RemoveProxyOps, AuthorID, Client).

%% Test committing an object that references a non-existent entity
commit_object_with_nonexistent_reference_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"commit_object_with_nonexistent_reference_test">>,
    AuthorID = create_author(Email, Client),

    % Try to create a provider that references a non-existent proxy
    Revision1 = 0,
    % This proxy doesn't exist
    NonExistentProxyRef = #domain_ProxyRef{id = 999999},

    ProviderOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {provider, #domain_Provider{
                    name = <<"provider_with_invalid_reference">>,
                    realm = test,
                    description = <<"provider that references non-existent proxy">>,
                    proxy = #domain_Proxy{
                        ref = NonExistentProxyRef,
                        additional = #{}
                    }
                }}
        }}
    ],

    {exception, #domain_conf_v2_OperationInvalid{
        errors = [
            {object_not_exists, #domain_conf_v2_NonexistantObject{
                object_ref = {proxy, _},
                referenced_by = [{provider, _}]
            }}
        ]
    }} = dmt_client:commit(Revision1, ProviderOps, AuthorID, Client).

%% Test CheckoutObjectWithReferences functionality
checkout_object_with_references_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"checkout_object_with_references_test">>,
    AuthorID = create_author(Email, Client),

    Revision1 = 0,

    % Step 1: Create a proxy object (the referenced object)
    ProxyOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {proxy, #domain_ProxyDefinition{
                    name = <<"test_proxy">>,
                    description = <<"proxy for checkout with references test">>,
                    url = <<"http://test-proxy.example.com">>,
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
    }} = dmt_client:commit(Revision1, ProxyOps, AuthorID, Client),

    % Step 2: Create a provider object that references the proxy
    ProviderOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {provider, #domain_Provider{
                    name = <<"test_provider">>,
                    realm = test,
                    description = <<"provider for checkout with references test">>,
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
                ref = ProviderRef
            }}
        ]
    }} = dmt_client:commit(Revision2, ProviderOps, AuthorID, Client),

    % Step 3: Create a category object that doesn't reference anything (for comparison)
    CategoryOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<"test_category">>,
                    description = <<"category for checkout with references test">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision4,
        new_objects = [
            {category, #domain_CategoryObject{
                ref = CategoryRef
            }}
        ]
    }} = dmt_client:commit(Revision3, CategoryOps, AuthorID, Client),

    % Step 4: Test checkout_object_with_references for the provider (should have references_to)
    {ok, #domain_conf_v2_VersionedObjectWithReferences{
        object = ProviderVersionedObject,
        referenced_by = ProviderReferencedBy,
        references_to = ProviderReferencesTo
    }} = dmt_client:checkout_object_with_references(
        {version, Revision4}, {provider, ProviderRef}, Client
    ),

    % Verify the main object is correct
    #domain_conf_v2_VersionedObject{
        object = {provider, ProviderData}
    } = ProviderVersionedObject,
    ?assertEqual(ProviderRef, ProviderData#domain_ProviderObject.ref),

    % Verify the provider references the proxy (references_to should contain proxy)
    ?assertEqual(
        1, length(ProviderReferencesTo), "Provider should reference one object (the proxy)"
    ),
    [ReferencedProxyObject] = ProviderReferencesTo,
    #domain_conf_v2_VersionedObject{
        object = {proxy, ReferencedProxyData}
    } = ReferencedProxyObject,
    ?assertEqual(ProxyRef, ReferencedProxyData#domain_ProxyObject.ref),

    % Verify no objects reference the provider yet (referenced_by should be empty)
    ?assertEqual([], ProviderReferencedBy, "Provider should not be referenced by any objects yet"),

    % Step 5: Test checkout_object_with_references for the proxy (should have referenced_by)
    {ok, #domain_conf_v2_VersionedObjectWithReferences{
        object = ProxyVersionedObject,
        referenced_by = ProxyReferencedBy,
        references_to = ProxyReferencesTo
    }} = dmt_client:checkout_object_with_references(
        {version, Revision4}, {proxy, ProxyRef}, Client
    ),

    % Verify the main object is correct
    #domain_conf_v2_VersionedObject{
        object = {proxy, ProxyData}
    } = ProxyVersionedObject,
    ?assertEqual(ProxyRef, ProxyData#domain_ProxyObject.ref),

    % Verify the proxy is referenced by the provider (referenced_by should contain provider)
    ?assertEqual(
        1, length(ProxyReferencedBy), "Proxy should be referenced by one object (the provider)"
    ),
    [ReferencingProviderObject] = ProxyReferencedBy,
    #domain_conf_v2_VersionedObject{
        object = {provider, ReferencingProviderData}
    } = ReferencingProviderObject,
    ?assertEqual(ProviderRef, ReferencingProviderData#domain_ProviderObject.ref),

    % Verify the proxy doesn't reference any other objects (references_to should be empty)
    ?assertEqual([], ProxyReferencesTo, "Proxy should not reference any other objects"),

    % Step 6: Test checkout_object_with_references for the category (should have empty references)
    {ok, #domain_conf_v2_VersionedObjectWithReferences{
        object = CategoryVersionedObject,
        referenced_by = CategoryReferencedBy,
        references_to = CategoryReferencesTo
    }} = dmt_client:checkout_object_with_references(
        {version, Revision4}, {category, CategoryRef}, Client
    ),

    % Verify the main object is correct
    #domain_conf_v2_VersionedObject{
        object = {category, CategoryData}
    } = CategoryVersionedObject,
    ?assertEqual(CategoryRef, CategoryData#domain_CategoryObject.ref),

    % Verify the category has no references (both lists should be empty)
    ?assertEqual([], CategoryReferencedBy, "Category should not be referenced by any objects"),
    ?assertEqual([], CategoryReferencesTo, "Category should not reference any other objects"),

    % Step 7: Test with head version reference
    {ok, #domain_conf_v2_VersionedObjectWithReferences{
        object = HeadProviderVersionedObject,
        referenced_by = HeadProviderReferencedBy,
        references_to = HeadProviderReferencesTo
    }} = dmt_client:checkout_object_with_references(
        {head, #domain_conf_v2_Head{}}, {provider, ProviderRef}, Client
    ),

    % Verify head version returns the same data as specific version
    ?assertEqual(ProviderVersionedObject, HeadProviderVersionedObject),
    ?assertEqual(ProviderReferencedBy, HeadProviderReferencedBy),
    ?assertEqual(ProviderReferencesTo, HeadProviderReferencesTo),

    % Step 8: Test error handling - nonexistent object
    NonexistentRef = {category, #domain_CategoryRef{id = 999999}},
    {exception, #domain_conf_v2_ObjectNotFound{}} =
        dmt_client:checkout_object_with_references(
            {version, Revision4}, NonexistentRef, Client
        ),

    % Step 9: Test error handling - nonexistent version
    {exception, #domain_conf_v2_VersionNotFound{}} =
        dmt_client:checkout_object_with_references(
            {version, 999999}, {provider, ProviderRef}, Client
        ).

%% Test GetRelatedGraph functionality - Basic test
get_related_graph_basic_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    Email = <<"get_related_graph_basic_test@example.com">>,
    AuthorID = create_author(Email, Client),
    Revision1 = 0,

    % Create a simple relationship chain: Category -> Provider -> Proxy
    % Step 1: Create a proxy
    ProxyOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {proxy, #domain_ProxyDefinition{
                    name = <<"basic_test_proxy">>,
                    description = <<"proxy for basic graph test">>,
                    url = <<"http://basic-test-proxy.example.com">>,
                    options = #{}
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision2,
        new_objects = [{proxy, #domain_ProxyObject{ref = ProxyRef}}]
    }} = dmt_client:commit(Revision1, ProxyOps, AuthorID, Client),

    % Step 2: Create a provider that references the proxy
    ProviderOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {provider, #domain_Provider{
                    name = <<"basic_test_provider">>,
                    realm = test,
                    description = <<"provider for basic graph test">>,
                    proxy = #domain_Proxy{ref = ProxyRef, additional = #{}}
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision3,
        new_objects = [{provider, #domain_ProviderObject{ref = ProviderRef}}]
    }} = dmt_client:commit(Revision2, ProviderOps, AuthorID, Client),

    % Step 3: Test GetRelatedGraph from provider perspective
    Request = #domain_conf_v2_RelatedGraphRequest{
        ref = {provider, ProviderRef},
        version = Revision3,
        type = undefined,
        include_inbound = true,
        include_outbound = true,
        depth = 1
    },

    {ok, #domain_conf_v2_RelatedGraph{
        nodes = Nodes,
        edges = Edges
    }} = dmt_client:get_related_graph(Request, Client),

    % Verify nodes
    ?assert(length(Nodes) >= 2, "Should have at least 2 nodes (provider and proxy)"),
    NodeRefs = [Ref || #domain_conf_v2_LimitedVersionedObject{ref = Ref} <- Nodes],
    ?assert(lists:member({provider, ProviderRef}, NodeRefs), "Should contain provider node"),
    ?assert(lists:member({proxy, ProxyRef}, NodeRefs), "Should contain proxy node"),

    % Verify edges
    _ = io:format("Edges: ~p~n", [Edges]),
    ?assert(length(Edges) >= 1, "Should have at least 1 edge"),
    EdgeSourceTargets = [
        {Source, Target}
     || #domain_conf_v2_ReferenceEdge{source = Source, target = Target} <- Edges
    ],
    ?assert(
        lists:member({{provider, ProviderRef}, {proxy, ProxyRef}}, EdgeSourceTargets),
        "Should have edge from provider to proxy"
    ).

%% Test GetRelatedGraph depth traversal
get_related_graph_depth_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    Email = <<"get_related_graph_depth_test@example.com">>,
    AuthorID = create_author(Email, Client),
    Revision1 = 0,

    % Create a chain: Provider -> Proxy -> Category
    % Step 1: Create category
    CategoryOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<"depth_test_category">>,
                    description = <<"category for depth test">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision2,
        new_objects = [{category, #domain_CategoryObject{ref = _CategoryRef}}]
    }} = dmt_client:commit(Revision1, CategoryOps, AuthorID, Client),

    % Step 2: Create proxy
    ProxyOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {proxy, #domain_ProxyDefinition{
                    name = <<"depth_test_proxy">>,
                    description = <<"proxy for depth test">>,
                    url = <<"http://depth-test-proxy.example.com">>,
                    options = #{}
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision3,
        new_objects = [{proxy, #domain_ProxyObject{ref = ProxyRef}}]
    }} = dmt_client:commit(Revision2, ProxyOps, AuthorID, Client),

    % Step 3: Create provider that references proxy
    ProviderOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {provider, #domain_Provider{
                    name = <<"depth_test_provider">>,
                    realm = test,
                    description = <<"provider for depth test">>,
                    proxy = #domain_Proxy{ref = ProxyRef, additional = #{}}
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision4,
        new_objects = [{provider, #domain_ProviderObject{ref = ProviderRef}}]
    }} = dmt_client:commit(Revision3, ProviderOps, AuthorID, Client),

    % Test with depth 1 - should only get direct neighbors
    RequestDepth1 = #domain_conf_v2_RelatedGraphRequest{
        ref = {provider, ProviderRef},
        version = Revision4,
        type = undefined,
        include_inbound = true,
        include_outbound = true,
        depth = 1
    },

    {ok, #domain_conf_v2_RelatedGraph{nodes = NodesDepth1}} = dmt_client:get_related_graph(
        RequestDepth1, Client
    ),

    NodeRefsDepth1 = [Ref || #domain_conf_v2_LimitedVersionedObject{ref = Ref} <- NodesDepth1],
    ?assert(
        lists:member({provider, ProviderRef}, NodeRefsDepth1),
        "Depth 1: Should contain provider node"
    ),
    ?assert(lists:member({proxy, ProxyRef}, NodeRefsDepth1), "Depth 1: Should contain proxy node"),

    % Test with depth 2 - should get deeper connections
    RequestDepth2 = #domain_conf_v2_RelatedGraphRequest{
        ref = {provider, ProviderRef},
        version = Revision4,
        type = undefined,
        include_inbound = true,
        include_outbound = true,
        depth = 2
    },

    {ok, #domain_conf_v2_RelatedGraph{nodes = NodesDepth2}} = dmt_client:get_related_graph(
        RequestDepth2, Client
    ),

    ?assert(
        length(NodesDepth2) >= length(NodesDepth1), "Depth 2 should have >= nodes than depth 1"
    ).

%% Test GetRelatedGraph inbound/outbound filtering
get_related_graph_inbound_outbound_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    Email = <<"get_related_graph_inbound_outbound_test@example.com">>,
    AuthorID = create_author(Email, Client),
    Revision1 = 0,

    % Create a bidirectional scenario: Category -> Provider -> Proxy
    % Step 1: Create proxy
    ProxyOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {proxy, #domain_ProxyDefinition{
                    name = <<"inbound_outbound_test_proxy">>,
                    description = <<"proxy for inbound/outbound test">>,
                    url = <<"http://inbound-outbound-test-proxy.example.com">>,
                    options = #{}
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision2,
        new_objects = [{proxy, #domain_ProxyObject{ref = ProxyRef}}]
    }} = dmt_client:commit(Revision1, ProxyOps, AuthorID, Client),

    % Step 2: Create provider that references proxy
    ProviderOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {provider, #domain_Provider{
                    name = <<"inbound_outbound_test_provider">>,
                    realm = test,
                    description = <<"provider for inbound/outbound test">>,
                    proxy = #domain_Proxy{ref = ProxyRef, additional = #{}}
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = FinalRevision,
        new_objects = [{provider, #domain_ProviderObject{ref = ProviderRef}}]
    }} = dmt_client:commit(Revision2, ProviderOps, AuthorID, Client),

    % Test outbound only (from provider perspective)
    OutboundRequest = #domain_conf_v2_RelatedGraphRequest{
        ref = {provider, ProviderRef},
        version = FinalRevision,
        type = undefined,
        include_inbound = false,
        include_outbound = true,
        depth = 1
    },

    {ok, #domain_conf_v2_RelatedGraph{nodes = OutboundNodes}} = dmt_client:get_related_graph(
        OutboundRequest, Client
    ),

    OutboundNodeRefs = [Ref || #domain_conf_v2_LimitedVersionedObject{ref = Ref} <- OutboundNodes],
    ?assert(
        lists:member({proxy, ProxyRef}, OutboundNodeRefs),
        "Outbound: Should contain proxy (referenced by provider)"
    ),

    % Test inbound only (from proxy perspective)
    InboundRequest = #domain_conf_v2_RelatedGraphRequest{
        ref = {proxy, ProxyRef},
        version = FinalRevision,
        type = undefined,
        include_inbound = true,
        include_outbound = false,
        depth = 1
    },

    {ok, #domain_conf_v2_RelatedGraph{nodes = InboundNodes}} = dmt_client:get_related_graph(
        InboundRequest, Client
    ),

    InboundNodeRefs = [Ref || #domain_conf_v2_LimitedVersionedObject{ref = Ref} <- InboundNodes],
    ?assert(
        lists:member({provider, ProviderRef}, InboundNodeRefs),
        "Inbound: Should contain provider (references proxy)"
    ).

%% Test GetRelatedGraph type filtering
get_related_graph_type_filter_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    Email = <<"get_related_graph_type_filter_test@example.com">>,
    AuthorID = create_author(Email, Client),
    Revision1 = 0,

    % Create a mixed environment with different types: Category, Provider, Proxy
    % Step 1: Create proxy
    ProxyOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {proxy, #domain_ProxyDefinition{
                    name = <<"type_filter_test_proxy">>,
                    description = <<"proxy for type filter test">>,
                    url = <<"http://type-filter-test-proxy.example.com">>,
                    options = #{}
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision2,
        new_objects = [{proxy, #domain_ProxyObject{ref = ProxyRef}}]
    }} = dmt_client:commit(Revision1, ProxyOps, AuthorID, Client),

    % Step 2: Create provider that references proxy
    ProviderOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {provider, #domain_Provider{
                    name = <<"type_filter_test_provider">>,
                    realm = test,
                    description = <<"provider for type filter test">>,
                    proxy = #domain_Proxy{ref = ProxyRef, additional = #{}}
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision3,
        new_objects = [{provider, #domain_ProviderObject{ref = ProviderRef}}]
    }} = dmt_client:commit(Revision2, ProviderOps, AuthorID, Client),

    % Step 3: Create category (unrelated to test filtering)
    CategoryOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<"type_filter_test_category">>,
                    description = <<"category for type filter test">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = FinalRevision,
        new_objects = [{category, #domain_CategoryObject{ref = CategoryRef}}]
    }} = dmt_client:commit(Revision3, CategoryOps, AuthorID, Client),

    % Test filtering by provider type only
    ProviderFilterRequest = #domain_conf_v2_RelatedGraphRequest{
        ref = {provider, ProviderRef},
        version = FinalRevision,
        type = provider,
        include_inbound = true,
        include_outbound = true,
        depth = 2
    },

    {ok, #domain_conf_v2_RelatedGraph{nodes = ProviderFilterNodes}} = dmt_client:get_related_graph(
        ProviderFilterRequest, Client
    ),

    ProviderFilterNodeRefs = [
        Ref
     || #domain_conf_v2_LimitedVersionedObject{ref = Ref} <- ProviderFilterNodes
    ],
    ?assert(
        lists:member({provider, ProviderRef}, ProviderFilterNodeRefs),
        "Provider filter: Should contain provider"
    ),
    % Should not contain proxy or category since we filtered by provider type
    ?assertNot(
        lists:member({proxy, ProxyRef}, ProviderFilterNodeRefs),
        "Provider filter: Should not contain proxy"
    ),
    ?assertNot(
        lists:member({category, CategoryRef}, ProviderFilterNodeRefs),
        "Provider filter: Should not contain category"
    ).

%% Test GetRelatedGraph with complex relationship chains
get_related_graph_complex_chain_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    Email = <<"get_related_graph_complex_chain_test@example.com">>,
    AuthorID = create_author(Email, Client),
    Revision1 = 0,

    % Create a more complex graph: A -> B -> C and A -> D -> C (diamond pattern)
    % Create business schedules (A, D), categories (B), and proxy (C)

    % Step 1: Create proxy (C)
    ProxyOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {proxy, #domain_ProxyDefinition{
                    name = <<"complex_test_proxy_c">>,
                    description = <<"proxy C for complex test">>,
                    url = <<"http://complex-test-proxy-c.example.com">>,
                    options = #{}
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision2,
        new_objects = [{proxy, #domain_ProxyObject{ref = ProxyRefC}}]
    }} = dmt_client:commit(Revision1, ProxyOps, AuthorID, Client),

    % Step 2: Create a provider (B) that references proxy (C)
    ProviderOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {provider, #domain_Provider{
                    name = <<"complex_test_provider_b">>,
                    realm = test,
                    description = <<"provider B for complex test">>,
                    proxy = #domain_Proxy{ref = ProxyRefC, additional = #{}}
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision3,
        new_objects = [{provider, #domain_ProviderObject{ref = ProviderRefB}}]
    }} = dmt_client:commit(Revision2, ProviderOps, AuthorID, Client),

    % Step 3: Create another provider (D) that also references proxy (C)
    ProviderOps2 = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {provider, #domain_Provider{
                    name = <<"complex_test_provider_d">>,
                    realm = test,
                    description = <<"provider D for complex test">>,
                    proxy = #domain_Proxy{ref = ProxyRefC, additional = #{}}
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = FinalRevision,
        new_objects = [{provider, #domain_ProviderObject{ref = ProviderRefD}}]
    }} = dmt_client:commit(Revision3, ProviderOps2, AuthorID, Client),

    % Test from proxy perspective - should see both providers
    ComplexRequest = #domain_conf_v2_RelatedGraphRequest{
        ref = {proxy, ProxyRefC},
        version = FinalRevision,
        type = undefined,
        include_inbound = true,
        include_outbound = true,
        depth = 1
    },

    {ok, #domain_conf_v2_RelatedGraph{
        nodes = ComplexNodes,
        edges = ComplexEdges
    }} = dmt_client:get_related_graph(ComplexRequest, Client),

    ComplexNodeRefs = [Ref || #domain_conf_v2_LimitedVersionedObject{ref = Ref} <- ComplexNodes],
    ?assert(
        lists:member({proxy, ProxyRefC}, ComplexNodeRefs), "Complex: Should contain proxy C"
    ),
    ?assert(
        lists:member({provider, ProviderRefB}, ComplexNodeRefs),
        "Complex: Should contain provider B"
    ),
    ?assert(
        lists:member({provider, ProviderRefD}, ComplexNodeRefs),
        "Complex: Should contain provider D"
    ),

    % Verify edges
    EdgeSourceTargets = [
        {Source, Target}
     || #domain_conf_v2_ReferenceEdge{source = Source, target = Target} <- ComplexEdges
    ],
    ?assert(
        lists:member({{provider, ProviderRefB}, {proxy, ProxyRefC}}, EdgeSourceTargets),
        "Complex: Should have edge from provider B to proxy C"
    ),
    ?assert(
        lists:member({{provider, ProviderRefD}, {proxy, ProxyRefC}}, EdgeSourceTargets),
        "Complex: Should have edge from provider D to proxy C"
    ).

%% Test GetRelatedGraph error handling
get_related_graph_error_handling_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"get_related_graph_error_test@example.com">>,
    AuthorID = create_author(Email, Client),

    % Create a valid object first
    CategoryOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<"error_test_category">>,
                    description = <<"category for error test">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = ValidVersion,
        new_objects = [{category, #domain_CategoryObject{ref = CategoryRef}}]
    }} = dmt_client:commit(0, CategoryOps, AuthorID, Client),

    % Test non-existent object
    NonExistentRequest = #domain_conf_v2_RelatedGraphRequest{
        ref = {category, #domain_CategoryRef{id = 1337}},
        version = ValidVersion,
        type = undefined,
        include_inbound = true,
        include_outbound = true,
        depth = 1
    },

    {exception, #domain_conf_v2_ObjectNotFound{}} = dmt_client:get_related_graph(
        NonExistentRequest, Client
    ),

    % Test with non-existent version
    InvalidVersionRequest = #domain_conf_v2_RelatedGraphRequest{
        ref = {category, CategoryRef},
        version = ValidVersion + 999999,
        type = undefined,
        include_inbound = true,
        include_outbound = true,
        depth = 1
    },

    {exception, #domain_conf_v2_VersionNotFound{}} = dmt_client:get_related_graph(
        InvalidVersionRequest, Client
    ).

%% Test removing referencing and referenced entities in the same commit succeeds
remove_referencing_and_referenced_same_commit_success_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    Email = <<"remove_referencing_and_referenced_same_commit_success_test">>,
    AuthorID = create_author(Email, Client),

    %% Step 1: Insert referenced entity (proxy)
    Revision0 = 0,
    ProxyOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {proxy, #domain_ProxyDefinition{
                    name = <<"proxy_for_same_commit_delete">>,
                    description = <<"proxy to be deleted in same commit">>,
                    url = <<"http://same-commit-delete.example.com">>,
                    options = #{}
                }}
        }}
    ],
    {ok, #domain_conf_v2_CommitResponse{
        version = Revision1,
        new_objects = [
            {proxy, #domain_ProxyObject{ref = ProxyRef}}
        ]
    }} = dmt_client:commit(Revision0, ProxyOps, AuthorID, Client),

    %% Step 2: Insert referencing entity (provider -> proxy)
    ProviderOps = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {provider, #domain_Provider{
                    name = <<"provider_for_same_commit_delete">>,
                    realm = test,
                    description = <<"provider to be deleted in same commit">>,
                    proxy = #domain_Proxy{ref = ProxyRef, additional = #{}}
                }}
        }}
    ],
    {ok, #domain_conf_v2_CommitResponse{
        version = Revision2,
        new_objects = [
            {provider, #domain_ProviderObject{ref = ProviderRef}}
        ]
    }} = dmt_client:commit(Revision1, ProviderOps, AuthorID, Client),

    %% Step 3: Remove both in the same commit (provider first, then proxy)
    RemoveOps = [
        {remove, #domain_conf_v2_RemoveOp{ref = {provider, ProviderRef}}},
        {remove, #domain_conf_v2_RemoveOp{ref = {proxy, ProxyRef}}}
    ],
    {ok, #domain_conf_v2_CommitResponse{version = _Revision3}} =
        dmt_client:commit(Revision2, RemoveOps, AuthorID, Client).

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
