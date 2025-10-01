-module(dmt_search_objects_tests_SUITE).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("damsel/include/dmsl_domain_thrift.hrl").
-include_lib("damsel/include/dmsl_limiter_config_thrift.hrl").
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
    search_basic_test/1,
    search_limit_config_test/1,
    search_with_type_filter_test/1,
    search_pagination_test/1,
    search_with_version_filter_test/1,
    search_multiple_terms_test/1,
    search_no_results_test/1,
    search_invalid_query_test/1,
    search_full_objects_basic_test/1,
    search_full_objects_with_filter_test/1,
    search_full_objects_pagination_test/1,
    search_objects_without_name_desc_test/1,
    search_deleted_objects_test/1,
    checkout_deleted_version_test/1,
    search_updated_object_deduplication_test/1
]).

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
        {group, search_tests}
    ].

%% Define test groups
groups() ->
    [
        {search_tests, [], [
            search_basic_test,
            search_limit_config_test,
            search_with_type_filter_test,
            search_pagination_test,
            search_with_version_filter_test,
            search_multiple_terms_test,
            search_no_results_test,
            search_objects_without_name_desc_test,
            search_invalid_query_test,
            search_full_objects_basic_test,
            search_full_objects_with_filter_test,
            search_full_objects_pagination_test,
            search_deleted_objects_test,
            checkout_deleted_version_test,
            search_updated_object_deduplication_test
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

% Basic search test with proper validation
search_basic_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Create author
    Email = <<"search_basic_test@test">>,
    AuthorID = create_author(Email, Client),

    % Create test data with searchable content
    Revision = 0,
    CategoryName = <<"unique_category_name">>,
    CategoryDesc = <<"searchable description text">>,

    Operations = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = CategoryName,
                    description = CategoryDesc
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Version,
        new_objects = NewObjects
    }} = dmt_client:commit(Revision, Operations, AuthorID, Client),

    % Extract the created category reference
    [{category, #domain_CategoryObject{ref = CategoryRef}}] = ordsets:to_list(NewObjects),

    % Test search by name
    NameRequest = #domain_conf_v2_SearchRequestParams{
        query = <<"unique_category">>,
        version = Version,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = NameResults,
        total_count = NameCount,
        continuation_token = NameToken
    }} = dmt_client:search_objects(NameRequest, Client),

    % Verify results
    ?assertEqual(1, NameCount, "Should find exactly one matching object"),
    ?assertEqual(undefined, NameToken, "Should not have continuation token"),

    % Validate result structure and content
    ?assertMatch(
        [
            #domain_conf_v2_LimitedVersionedObject{
                ref = {category, CategoryRef},
                info = #domain_conf_v2_VersionedObjectInfo{
                    version = Version,
                    % Timestamp will vary
                    changed_at = _,
                    % Author object
                    changed_by = _
                },
                name = CategoryName,
                description = CategoryDesc
            }
        ],
        NameResults
    ),

    % Test search by description
    DescRequest = #domain_conf_v2_SearchRequestParams{
        query = <<"searchable description">>,
        version = Version,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = DescResults,
        total_count = DescCount
    }} = dmt_client:search_objects(DescRequest, Client),

    % Verify results
    ?assertEqual(1, DescCount, "Should find exactly one matching object"),

    % Should be the same object as in the name search
    ?assertEqual(
        NameResults, DescResults, "Same object should be found by both name and description"
    ).

search_limit_config_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    AuthorID = create_author(<<"search_limit_config_test@test">>, Client),
    Operations = [
        {insert, #domain_conf_v2_InsertOp{
            force_ref = {limit_config, #domain_LimitConfigRef{id = <<"search_limit_config_test">>}},
            object =
                {limit_config, #limiter_config_LimitConfig{
                    processor_type = <<"test">>,
                    started_at = <<"2021-01-01T00:00:00Z">>,
                    shard_size = 100,
                    time_range_type = {calendar, {day, #limiter_config_TimeRangeTypeCalendarDay{}}},
                    context_type =
                        {payment_processing, #limiter_config_LimitContextTypePaymentProcessing{}}
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Version
    }} = dmt_client:commit(0, Operations, AuthorID, Client),

    SearchRequest = #domain_conf_v2_SearchRequestParams{
        query = <<"*">>,
        version = Version,
        type = limit_config,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = Results
    }} = dmt_client:search_objects(SearchRequest, Client),
    ?assertEqual(1, length(Results), "Should find exactly one matching object").

% Search with type filter
search_with_type_filter_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Create author
    Email = <<"search_with_type_filter_test@test">>,
    AuthorID = create_author(Email, Client),

    % Create mixed test data (categories and terminals)
    Revision = 0,
    SearchTerm = <<"filterable">>,

    Operations = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<SearchTerm/binary, " category">>,
                    description = <<"A test category">>
                }}
        }},
        {insert, #domain_conf_v2_InsertOp{
            object =
                {terminal, #domain_Terminal{
                    name = <<SearchTerm/binary, " terminal">>,
                    description = <<"A test terminal">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Version
    }} = dmt_client:commit(Revision, Operations, AuthorID, Client),

    % Search without type filter (should find both objects)
    NoFilterRequest = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = NoFilterResults,
        total_count = NoFilterCount
    }} = dmt_client:search_objects(NoFilterRequest, Client),

    ?assertEqual(2, NoFilterCount, "Should find both objects without type filter"),

    % Verify we have both a category and terminal
    Types = lists:usort([
        Type
     || #domain_conf_v2_LimitedVersionedObject{
            ref = {Type, _}
        } <- NoFilterResults
    ]),
    ?assertEqual([category, terminal], lists:sort(Types), "Should find both types without filter"),

    % Search with category filter
    CategoryRequest = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version,
        limit = 10,
        type = category
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = CategoryResults,
        total_count = CategoryCount
    }} = dmt_client:search_objects(CategoryRequest, Client),

    ?assertEqual(1, CategoryCount, "Should find only the category with type filter"),

    % Verify we only have a category
    [
        #domain_conf_v2_LimitedVersionedObject{
            ref = {ResultType, _}
        }
    ] = CategoryResults,
    ?assertEqual(category, ResultType, "Should only find category with category filter"),

    % Search with terminal filter
    TerminalRequest = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version,
        limit = 10,
        type = terminal
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = _TerminalResults,
        total_count = TerminalCount
    }} = dmt_client:search_objects(TerminalRequest, Client),

    ?assertEqual(1, TerminalCount, "Should find only the terminal with type filter").

% Test pagination with continuation tokens
search_pagination_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Create author
    Email = <<"search_pagination_test@test">>,
    AuthorID = create_author(Email, Client),

    % Create many objects to trigger pagination
    Revision = 0,
    SearchTerm = <<"pageable">>,
    % Create enough objects to span multiple pages
    NumObjects = 5,

    Operations = lists:map(
        fun(N) ->
            {insert, #domain_conf_v2_InsertOp{
                object =
                    {category, #domain_Category{
                        name = list_to_binary(io_lib:format("~s category ~p", [SearchTerm, N])),
                        description = <<"For pagination testing">>
                    }}
            }}
        end,
        lists:seq(1, NumObjects)
    ),

    {ok, #domain_conf_v2_CommitResponse{
        version = Version
    }} = dmt_client:commit(Revision, Operations, AuthorID, Client),

    % Search with a small limit to force pagination
    PageSize = 2,
    Request1 = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version,
        limit = PageSize
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = Page1Results,
        total_count = Page1Count,
        continuation_token = Token1
    }} = dmt_client:search_objects(Request1, Client),

    % Verify first page
    ?assertEqual(PageSize, Page1Count, "First page should contain exactly PageSize items"),
    ?assertEqual(PageSize, length(Page1Results)),
    ?assertNotEqual(undefined, Token1, "Should have a continuation token for more results"),

    % Get second page
    Request2 = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version,
        limit = PageSize,
        continuation_token = Token1
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = Page2Results,
        total_count = Page2Count,
        continuation_token = Token2
    }} = dmt_client:search_objects(Request2, Client),

    % Verify second page
    ?assertEqual(PageSize, Page2Count, "Second page should contain exactly PageSize items"),
    ?assertEqual(PageSize, length(Page2Results)),
    ?assertNotEqual(undefined, Token2, "Should have a continuation token for more results"),

    % Get third page (should be partial)
    Request3 = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version,
        limit = PageSize,
        continuation_token = Token2
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = Page3Results,
        total_count = Page3Count,
        continuation_token = Token3
    }} = dmt_client:search_objects(Request3, Client),

    % Verify third page (last page)
    ExpectedRemaining = NumObjects - (2 * PageSize),
    ?assertEqual(ExpectedRemaining, Page3Count, "Last page should contain remaining items"),
    ?assertEqual(ExpectedRemaining, length(Page3Results)),
    ?assertEqual(undefined, Token3, "Should not have a continuation token after last page"),

    % Verify we got all objects and no duplicates
    AllResults = Page1Results ++ Page2Results ++ Page3Results,
    ?assertEqual(
        NumObjects, length(AllResults), "Should retrieve all created objects across pages"
    ),

    % Check for duplicates
    AllRefs = [
        Ref
     || #domain_conf_v2_LimitedVersionedObject{
            ref = Ref
        } <- AllResults
    ],
    UniqueRefs = lists:usort(AllRefs),
    ?assertEqual(
        length(AllRefs), length(UniqueRefs), "Should not have any duplicate results across pages"
    ).

% Test search with version filter
search_with_version_filter_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Create author
    Email = <<"search_with_version_filter_test@test">>,
    AuthorID = create_author(Email, Client),

    % Create objects in multiple versions
    SearchTerm = <<"versioned">>,

    % First commit - create first object
    Revision0 = 0,
    Operations1 = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<SearchTerm/binary, " category v1">>,
                    description = <<"First version">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision1
    }} = dmt_client:commit(Revision0, Operations1, AuthorID, Client),

    % Second commit - create second object
    Operations2 = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<SearchTerm/binary, " category v2">>,
                    description = <<"Second version">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision2
    }} = dmt_client:commit(Revision1, Operations2, AuthorID, Client),

    % Third commit - create third object
    Operations3 = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<SearchTerm/binary, " category v3">>,
                    description = <<"Third version">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Revision3
    }} = dmt_client:commit(Revision2, Operations3, AuthorID, Client),

    % Search at version 1 - should find only the first object
    RequestV1 = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Revision1,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = ResultsV1,
        total_count = CountV1
    }} = dmt_client:search_objects(RequestV1, Client),

    ?assertEqual(1, CountV1, "Should find only one object at version 1"),
    [
        #domain_conf_v2_LimitedVersionedObject{
            ref = {category, _},
            info = #domain_conf_v2_VersionedObjectInfo{
                version = V1
            }
        }
    ] = ResultsV1,
    ?assertEqual(Revision1, V1, "Object should be from version 1"),

    % Search at version 2 - should find first and second objects
    RequestV2 = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Revision2,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = ResultsV2,
        total_count = CountV2
    }} = dmt_client:search_objects(RequestV2, Client),

    ?assertEqual(2, CountV2, "Should find two objects at version 2"),
    Versions2 = lists:usort([
        V
     || #domain_conf_v2_LimitedVersionedObject{
            ref = {category, _},
            info = #domain_conf_v2_VersionedObjectInfo{
                version = V
            }
        } <- ResultsV2
    ]),
    ?assertEqual(
        [Revision1, Revision2], lists:sort(Versions2), "Should have objects from versions 1 and 2"
    ),

    % Search at version 3 (latest) - should find all three objects
    RequestV3 = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Revision3,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = ResultsV3,
        total_count = CountV3
    }} = dmt_client:search_objects(RequestV3, Client),

    ?assertEqual(3, CountV3, "Should find three objects at version 3"),
    Versions3 = lists:usort([
        V
     || #domain_conf_v2_LimitedVersionedObject{
            ref = {category, _},
            info = #domain_conf_v2_VersionedObjectInfo{
                version = V
            }
        } <- ResultsV3
    ]),
    ?assertEqual(
        [Revision1, Revision2, Revision3],
        lists:sort(Versions3),
        "Should have objects from all three versions"
    ).

% Test searching with complex query terms
search_multiple_terms_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Create author
    Email = <<"search_multiple_terms_test@test">>,
    AuthorID = create_author(Email, Client),

    % Create test data with specific terms
    Revision = 0,
    Term1 = <<"apple">>,
    Term2 = <<"banana">>,

    Operations = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<Term1/binary, " only">>,
                    description = <<"Contains only the first term">>
                }}
        }},
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<Term2/binary, " only">>,
                    description = <<"Contains only the second term">>
                }}
        }},
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<Term1/binary, " and ", Term2/binary>>,
                    description = <<"Contains both terms">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Version
    }} = dmt_client:commit(Revision, Operations, AuthorID, Client),

    % Search for first term only
    Request1 = #domain_conf_v2_SearchRequestParams{
        query = Term1,
        version = Version,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = Results1,
        total_count = Count1
    }} = dmt_client:search_objects(Request1, Client),

    ?assertEqual(2, Count1, "Should find two objects with first term"),

    % Search for second term only
    Request2 = #domain_conf_v2_SearchRequestParams{
        query = Term2,
        version = Version,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = Results2,
        total_count = Count2
    }} = dmt_client:search_objects(Request2, Client),

    ?assertEqual(2, Count2, "Should find two objects with second term"),

    % Search for both terms (AND query)
    Request3 = #domain_conf_v2_SearchRequestParams{
        query = <<Term1/binary, " & ", Term2/binary>>,
        version = Version,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = Results3,
        total_count = Count3
    }} = dmt_client:search_objects(Request3, Client),

    ?assertEqual(1, Count3, "Should find one object with both terms"),

    % The object found by the AND query should be included in both single-term result sets
    [
        #domain_conf_v2_LimitedVersionedObject{
            ref = BothTermsRef
        }
    ] = Results3,
    ?assert(
        lists:any(
            fun(
                #domain_conf_v2_LimitedVersionedObject{
                    ref = Ref
                }
            ) ->
                Ref =:= BothTermsRef
            end,
            Results1
        ),
        "Object with both terms should be found in first term search"
    ),
    ?assert(
        lists:any(
            fun(
                #domain_conf_v2_LimitedVersionedObject{
                    ref = Ref
                }
            ) ->
                Ref =:= BothTermsRef
            end,
            Results2
        ),
        "Object with both terms should be found in second term search"
    ).

% Test search with no results
search_no_results_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Create author
    Email = <<"search_no_results_test@test">>,
    AuthorID = create_author(Email, Client),

    % Create a test object to verify search works
    Revision = 0,
    Operations = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<"standard test category">>,
                    description = <<"A basic test object">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Version
    }} = dmt_client:commit(Revision, Operations, AuthorID, Client),

    % Search for a non-existent term
    NonExistentTerm = <<"xyzzy123nonexistent">>,
    Request = #domain_conf_v2_SearchRequestParams{
        query = NonExistentTerm,
        version = Version,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = Results,
        total_count = Count,
        continuation_token = Token
    }} = dmt_client:search_objects(Request, Client),

    % Verify empty results
    ?assertEqual(0, Count, "Should find no results with non-existent term"),
    ?assertEqual([], Results, "Result list should be empty"),
    ?assertEqual(undefined, Token, "Continuation token should be undefined for empty results").

% Test invalid query handling
search_invalid_query_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    SacrificialType = dummy,
    {ok, _} = epg_pool:query(
        default_pool,
        """
        DELETE FROM entity_type
        WHERE name = $1;
        """,
        [SacrificialType]
    ),

    % Search with invalid object type
    InvalidTypeRequest = #domain_conf_v2_SearchRequestParams{
        query = <<"test">>,
        version = 0,
        limit = 10,
        type = SacrificialType
    },

    % Expect object type not found error
    Result = dmt_client:search_objects(InvalidTypeRequest, Client),
    ?assertMatch({exception, #domain_conf_v2_ObjectTypeNotFound{}}, Result).

%% Tests for SearchFullObjects

% Basic test of SearchFullObjects functionality
search_full_objects_basic_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Create author
    Email = <<"search_full_objects_basic_test@test">>,
    AuthorID = create_author(Email, Client),

    % Create test data with searchable content
    Revision = 0,
    CategoryName = <<"search_full_category">>,
    CategoryDesc = <<"searchable full object text">>,

    Operations = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = CategoryName,
                    description = CategoryDesc
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Version,
        new_objects = NewObjects
    }} = dmt_client:commit(Revision, Operations, AuthorID, Client),

    % Extract the created category reference
    [{category, #domain_CategoryObject{ref = CategoryRef}}] = ordsets:to_list(NewObjects),

    % Search for full objects
    NameRequest = #domain_conf_v2_SearchRequestParams{
        query = <<"search_full_category">>,
        version = Version,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchFullResponse{
        result = NameResults,
        total_count = NameCount,
        continuation_token = NameToken
    }} = dmt_client:search_full_objects(NameRequest, Client),

    % Verify results
    ?assertEqual(1, NameCount, "Should find exactly one matching object"),
    ?assertEqual(undefined, NameToken, "Should not have continuation token"),

    % Validate result structure and content
    [FullObject] = NameResults,
    ?assertMatch(
        #domain_conf_v2_VersionedObject{
            info = #domain_conf_v2_VersionedObjectInfo{
                version = Version
            },
            object =
                {category, #domain_CategoryObject{
                    ref = CategoryRef,
                    data = #domain_Category{
                        name = CategoryName,
                        description = CategoryDesc
                    }
                }}
        },
        FullObject
    ),

    % Search by description
    DescRequest = #domain_conf_v2_SearchRequestParams{
        query = <<"searchable full object">>,
        version = Version,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchFullResponse{
        result = DescResults,
        total_count = DescCount
    }} = dmt_client:search_full_objects(DescRequest, Client),

    % Verify results
    ?assertEqual(1, DescCount, "Should find exactly one matching object"),
    ?assertEqual(
        NameResults, DescResults, "Same full object should be found by both name and description"
    ).

% Search full objects with type filter
search_full_objects_with_filter_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Create author
    Email = <<"search_full_objects_with_filter_test@test">>,
    AuthorID = create_author(Email, Client),

    % Create mixed test data (categories and terminals)
    Revision = 0,
    SearchTerm = <<"filterable_full">>,

    Operations = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = <<SearchTerm/binary, " category">>,
                    description = <<"A test category for full objects">>
                }}
        }},
        {insert, #domain_conf_v2_InsertOp{
            object =
                {terminal, #domain_Terminal{
                    name = <<SearchTerm/binary, " terminal">>,
                    description = <<"A test terminal for full objects">>
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Version
    }} = dmt_client:commit(Revision, Operations, AuthorID, Client),

    % Search without type filter (should find both objects)
    NoFilterRequest = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchFullResponse{
        result = NoFilterResults,
        total_count = NoFilterCount
    }} = dmt_client:search_full_objects(NoFilterRequest, Client),

    ?assertEqual(2, NoFilterCount, "Should find both full objects without type filter"),

    % Verify we have both a category and terminal
    Types = lists:usort([
        Type
     || #domain_conf_v2_VersionedObject{
            object = {Type, _}
        } <- NoFilterResults
    ]),
    ?assertEqual([category, terminal], lists:sort(Types), "Should find both types without filter"),

    % Search with category filter
    CategoryRequest = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version,
        limit = 10,
        type = category
    },

    {ok, #domain_conf_v2_SearchFullResponse{
        result = CategoryResults,
        total_count = CategoryCount
    }} = dmt_client:search_full_objects(CategoryRequest, Client),

    ?assertEqual(1, CategoryCount, "Should find only the category with type filter"),

    % Verify we only have a category
    [
        #domain_conf_v2_VersionedObject{
            object = {ResultType, _}
        }
    ] = CategoryResults,
    ?assertEqual(category, ResultType, "Should only find category with category filter"),

    % Search with terminal filter
    TerminalRequest = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version,
        limit = 10,
        type = terminal
    },

    {ok, #domain_conf_v2_SearchFullResponse{
        result = TerminalResults,
        total_count = TerminalCount
    }} = dmt_client:search_full_objects(TerminalRequest, Client),

    ?assertEqual(1, TerminalCount, "Should find only the terminal with type filter"),

    % Verify we only have a terminal
    [
        #domain_conf_v2_VersionedObject{
            object = {ResultType2, _}
        }
    ] = TerminalResults,
    ?assertEqual(terminal, ResultType2, "Should only find terminal with terminal filter").

% Test pagination of full objects
search_full_objects_pagination_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Create author
    Email = <<"search_full_objects_pagination_test@test">>,
    AuthorID = create_author(Email, Client),

    % Create many objects to trigger pagination
    Revision = 0,
    SearchTerm = <<"pageable_full">>,
    % Create enough objects to span multiple pages
    NumObjects = 5,

    Operations = lists:map(
        fun(N) ->
            {insert, #domain_conf_v2_InsertOp{
                object =
                    {category, #domain_Category{
                        name = list_to_binary(io_lib:format("~s category ~p", [SearchTerm, N])),
                        description = <<"For full objects pagination testing">>
                    }}
            }}
        end,
        lists:seq(1, NumObjects)
    ),

    {ok, #domain_conf_v2_CommitResponse{
        version = Version
    }} = dmt_client:commit(Revision, Operations, AuthorID, Client),

    % Search with a small limit to force pagination
    PageSize = 2,
    Request1 = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version,
        limit = PageSize
    },

    {ok, #domain_conf_v2_SearchFullResponse{
        result = Page1Results,
        total_count = Page1Count,
        continuation_token = Token1
    }} = dmt_client:search_full_objects(Request1, Client),

    % Verify first page
    ?assertEqual(PageSize, Page1Count, "First page should contain exactly PageSize items"),
    ?assertEqual(PageSize, length(Page1Results)),
    ?assertNotEqual(undefined, Token1, "Should have a continuation token for more results"),

    % Get second page
    Request2 = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version,
        limit = PageSize,
        continuation_token = Token1
    },

    {ok, #domain_conf_v2_SearchFullResponse{
        result = Page2Results,
        total_count = Page2Count,
        continuation_token = Token2
    }} = dmt_client:search_full_objects(Request2, Client),

    % Verify second page
    ?assertEqual(PageSize, Page2Count, "Second page should contain exactly PageSize items"),
    ?assertEqual(PageSize, length(Page2Results)),
    ?assertNotEqual(undefined, Token2, "Should have a continuation token for more results"),

    % Get third page (should be partial)
    Request3 = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version,
        limit = PageSize,
        continuation_token = Token2
    },

    {ok, #domain_conf_v2_SearchFullResponse{
        result = Page3Results,
        total_count = Page3Count,
        continuation_token = Token3
    }} = dmt_client:search_full_objects(Request3, Client),

    % Verify third page (last page)
    ExpectedRemaining = NumObjects - (2 * PageSize),
    ?assertEqual(ExpectedRemaining, Page3Count, "Last page should contain remaining items"),
    ?assertEqual(ExpectedRemaining, length(Page3Results)),
    ?assertEqual(undefined, Token3, "Should not have a continuation token after last page"),

    % Verify we got all objects and no duplicates
    AllResults = Page1Results ++ Page2Results ++ Page3Results,
    ?assertEqual(
        NumObjects, length(AllResults), "Should retrieve all created full objects across pages"
    ),

    % Check for duplicates by extracting refs
    AllRefs = [
        Ref
     || #domain_conf_v2_VersionedObject{
            object = {_, #domain_DummyObject{ref = Ref}}
        } <- AllResults
    ],
    UniqueRefs = lists:usort(AllRefs),
    ?assertEqual(
        length(AllRefs), length(UniqueRefs), "Should not have any duplicate results across pages"
    ).

% Test searching for objects without name or description fields
search_objects_without_name_desc_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Create author
    Email = <<"search_objects_without_name_desc_test@test">>,
    AuthorID = create_author(Email, Client),

    % Ensure the 'dummy' entity type exists in the database, it could be deleted by previous tests
    {ok, _} = epg_pool:query(
        default_pool,
        """
        INSERT INTO entity_type (name, has_sequence)
        VALUES ($1, FALSE)
        ON CONFLICT (name) DO NOTHING;
        """,
        [dummy]
    ),

    % Create a Dummy object which doesn't have name or description fields
    Revision = 0,
    DummyRef = #domain_DummyRef{id = <<"search_objects_without_name_desc_test">>},
    Operations = [
        {insert, #domain_conf_v2_InsertOp{
            force_ref = {dummy, DummyRef},
            object = {dummy, #domain_Dummy{}}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Version,
        new_objects = NewObjects
    }} = dmt_client:commit(Revision, Operations, AuthorID, Client),

    % Extract the created dummy reference
    [{dummy, #domain_DummyObject{ref = DummyRef}}] = ordsets:to_list(NewObjects),

    % Search for dummy objects (should find by type since there's no name/description)
    Request = #domain_conf_v2_SearchRequestParams{
        % This should match the type name
        query = <<"dummy">>,
        version = Version,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = Results,
        total_count = Count
    }} = dmt_client:search_objects(Request, Client),

    % We should still find the object even though it doesn't have name/description
    ?assertEqual(1, Count, "Should find the dummy object"),

    % Verify the result structure
    ?assertMatch(
        [
            #domain_conf_v2_LimitedVersionedObject{
                ref = {dummy, DummyRef},
                info = #domain_conf_v2_VersionedObjectInfo{
                    version = Version
                },
                name = undefined,
                description = undefined
            }
        ],
        Results,
        "Should return the dummy object with undefined name and description"
    ),

    % Also search with explicit type filter
    TypedRequest = #domain_conf_v2_SearchRequestParams{
        % Empty query but with type filter
        query = <<"*">>,
        version = Version,
        limit = 10,
        type = dummy
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = TypedResults,
        total_count = TypedCount
    }} = dmt_client:search_objects(TypedRequest, Client),

    % Should find by type even with empty query
    ?assertEqual(1, TypedCount, "Should find the dummy object by type filter"),
    ?assertEqual(Results, TypedResults, "Same result should be returned for type-based search"),

    % Also test with full objects search
    {ok, #domain_conf_v2_SearchFullResponse{
        result = FullResults,
        total_count = FullCount
    }} = dmt_client:search_full_objects(Request, Client),

    % Verify full object search works too
    ?assertEqual(1, FullCount, "Should find the dummy object in full search"),
    ?assertMatch(
        [
            #domain_conf_v2_VersionedObject{
                info = #domain_conf_v2_VersionedObjectInfo{
                    version = Version
                },
                object =
                    {dummy, #domain_DummyObject{
                        ref = DummyRef,
                        data = #domain_Dummy{}
                    }}
            }
        ],
        FullResults,
        "Should return the full dummy object"
    ).

% Test searching for deleted objects
search_deleted_objects_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Create author
    Email = <<"search_deleted_objects_test@test">>,
    AuthorID = create_author(Email, Client),

    % Commit 1: Insert objects that will be deleted
    Revision0 = 0,
    SearchTerm = <<"deletable">>,
    CategoryName1 = <<SearchTerm/binary, " category 1">>,
    CategoryDesc1 = <<"First deletable category">>,
    CategoryName2 = <<SearchTerm/binary, " category 2">>,
    CategoryDesc2 = <<"Second deletable category">>,

    InsertOperations = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = CategoryName1,
                    description = CategoryDesc1
                }}
        }},
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = CategoryName2,
                    description = CategoryDesc2
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Version1,
        new_objects = NewObjects1
    }} = dmt_client:commit(Revision0, InsertOperations, AuthorID, Client),

    % Extract the created category references
    NewObjectsList = ordsets:to_list(NewObjects1),
    % Find the references by matching the category names
    [
        {category, #domain_CategoryObject{ref = Ref1}},
        {category, #domain_CategoryObject{ref = Ref2}}
    ] = NewObjectsList,

    % Get the actual objects to check their names
    {ok, Obj1} = dmt_client:checkout_object({version, Version1}, {category, Ref1}, Client),
    {ok, Obj2} = dmt_client:checkout_object({version, Version1}, {category, Ref2}, Client),

    #domain_conf_v2_VersionedObject{
        object =
            {category, #domain_CategoryObject{
                data = #domain_Category{name = Name1}
            }}
    } = Obj1,
    #domain_conf_v2_VersionedObject{
        object =
            {category, #domain_CategoryObject{
                data = #domain_Category{name = Name2}
            }}
    } = Obj2,

    {CategoryRef1, CategoryRef2} =
        case {Name1, Name2} of
            {CategoryName1, CategoryName2} -> {Ref1, Ref2};
            {CategoryName2, CategoryName1} -> {Ref2, Ref1}
        end,

    % Verify objects can be found before deletion
    SearchRequestV1 = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version1,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = ResultsV1,
        total_count = CountV1
    }} = dmt_client:search_objects(SearchRequestV1, Client),

    ?assertEqual(2, CountV1, "Should find both objects before deletion"),
    ?assertEqual(2, length(ResultsV1), "Should return both objects before deletion"),

    % Commit 2: Delete one of the objects
    RemoveOperations = [
        {remove, #domain_conf_v2_RemoveOp{
            ref = {category, CategoryRef1}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Version2
    }} = dmt_client:commit(Version1, RemoveOperations, AuthorID, Client),

    % Verify only one object can be found at current version after deletion
    SearchRequestV2 = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version2,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = ResultsV2,
        total_count = CountV2
    }} = dmt_client:search_objects(SearchRequestV2, Client),

    ?assertEqual(1, CountV2, "Should find only one object after deletion"),
    ?assertEqual(1, length(ResultsV2), "Should return only one object after deletion"),

    % The remaining object should be the second one
    [
        #domain_conf_v2_LimitedVersionedObject{
            ref = {category, RemainingRef},
            name = RemainingName
        }
    ] = ResultsV2,
    ?assertEqual(CategoryRef2, RemainingRef, "Should find the second (non-deleted) category"),
    ?assertEqual(CategoryName2, RemainingName, "Should have correct name for remaining object"),

    % Verify we can still find the deleted object when searching at the pre-deletion version
    SearchRequestAtV1 = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version1,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = ResultsAtV1,
        total_count = CountAtV1
    }} = dmt_client:search_objects(SearchRequestAtV1, Client),

    ?assertEqual(
        2, CountAtV1, "Should still find both objects when searching at pre-deletion version"
    ),
    ?assertEqual(2, length(ResultsAtV1), "Should return both objects at pre-deletion version"),

    % Verify the deleted object specifically can be found by searching for its unique name at Version1
    DeletedObjectSearchV1 = #domain_conf_v2_SearchRequestParams{
        query = <<"category 1">>,
        version = Version1,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = DeletedResultsV1,
        total_count = DeletedCountV1
    }} = dmt_client:search_objects(DeletedObjectSearchV1, Client),

    ?assertEqual(
        1, DeletedCountV1, "Should find deleted object when searching at pre-deletion version"
    ),
    [
        #domain_conf_v2_LimitedVersionedObject{
            ref = {category, DeletedRef}
        }
    ] = DeletedResultsV1,
    ?assertEqual(CategoryRef1, DeletedRef, "Should find the correct deleted object"),

    % Verify the deleted object cannot be found by searching for its unique name at Version2
    DeletedObjectSearchV2 = #domain_conf_v2_SearchRequestParams{
        query = <<"category 1">>,
        version = Version2,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = DeletedResultsV2,
        total_count = DeletedCountV2
    }} = dmt_client:search_objects(DeletedObjectSearchV2, Client),

    ?assertEqual(
        0, DeletedCountV2, "Should not find deleted object when searching at post-deletion version"
    ),
    ?assertEqual(
        [], DeletedResultsV2, "Should return empty results for deleted object at current version"
    ),

    % Test the same behavior with full objects search
    {ok, #domain_conf_v2_SearchFullResponse{
        result = _FullResultsV1,
        total_count = FullCountV1
    }} = dmt_client:search_full_objects(SearchRequestV1, Client),

    ?assertEqual(2, FullCountV1, "Full search should find both objects before deletion"),

    {ok, #domain_conf_v2_SearchFullResponse{
        result = FullResultsV2,
        total_count = FullCountV2
    }} = dmt_client:search_full_objects(SearchRequestV2, Client),

    ?assertEqual(1, FullCountV2, "Full search should find only one object after deletion"),
    ?assertEqual(
        1, length(FullResultsV2), "Full search should return only one object after deletion"
    ).

% Test that checking out an object at a version where it was subsequently deleted fails
checkout_deleted_version_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Create author
    Email = <<"checkout_deleted_version_test@test">>,
    AuthorID = create_author(Email, Client),

    % Commit 1: Insert an object
    Revision0 = 0,
    CategoryName = <<"category_to_be_deleted">>,
    CategoryDesc = <<"This category will be deleted">>,

    InsertOperations = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = CategoryName,
                    description = CategoryDesc
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Version1,
        new_objects = NewObjects1
    }} = dmt_client:commit(Revision0, InsertOperations, AuthorID, Client),

    [{category, #domain_CategoryObject{ref = CategoryRef}}] = ordsets:to_list(NewObjects1),

    % Commit 2: Remove the object
    RemoveOperations = [
        {remove, #domain_conf_v2_RemoveOp{
            ref = {category, CategoryRef}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        % Version after removal
        version = Version2
    }} = dmt_client:commit(Version1, RemoveOperations, AuthorID, Client),
    % Ensure version incremented
    ?assertNotEqual(Version1, Version2),

    % Attempt to checkout the object at Version1 (where it existed but was later removed)
    % We expect this to fail because the object is no longer considered valid at Version1
    % after the removal operation in Version2.
    CheckoutResult = dmt_client:checkout_object(
        {version, Version2}, {category, CategoryRef}, Client
    ),

    ?assertMatch(
        {exception, #domain_conf_v2_ObjectNotFound{}},
        CheckoutResult,
        "Should fail with ObjectNotFound when checking out a version that has been deleted"
    ).

% Test updated object deduplication
search_updated_object_deduplication_test(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    % Create author
    Email = <<"search_updated_object_deduplication_test@test">>,
    AuthorID = create_author(Email, Client),

    % Commit 1: Create initial object
    Revision0 = 0,
    SearchTerm = <<"updatable_object">>,
    InitialName = <<SearchTerm/binary, " version 1">>,
    InitialDesc = <<"Initial description">>,

    InsertOperations = [
        {insert, #domain_conf_v2_InsertOp{
            object =
                {category, #domain_Category{
                    name = InitialName,
                    description = InitialDesc
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Version1,
        new_objects = NewObjects1
    }} = dmt_client:commit(Revision0, InsertOperations, AuthorID, Client),

    % Extract the created category reference
    [{category, #domain_CategoryObject{ref = CategoryRef}}] = ordsets:to_list(NewObjects1),

    % Commit 2: Update the same object
    UpdatedName1 = <<SearchTerm/binary, " version 2">>,
    UpdatedDesc1 = <<"Updated description first time">>,

    UpdateOperations1 = [
        {update, #domain_conf_v2_UpdateOp{
            object =
                {category, #domain_CategoryObject{
                    ref = CategoryRef,
                    data = #domain_Category{
                        name = UpdatedName1,
                        description = UpdatedDesc1
                    }
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Version2
    }} = dmt_client:commit(Version1, UpdateOperations1, AuthorID, Client),

    % Commit 3: Update the same object again
    UpdatedName2 = <<SearchTerm/binary, " version 3">>,
    UpdatedDesc2 = <<"Updated description second time">>,

    UpdateOperations2 = [
        {update, #domain_conf_v2_UpdateOp{
            object =
                {category, #domain_CategoryObject{
                    ref = CategoryRef,
                    data = #domain_Category{
                        name = UpdatedName2,
                        description = UpdatedDesc2
                    }
                }}
        }}
    ],

    {ok, #domain_conf_v2_CommitResponse{
        version = Version3
    }} = dmt_client:commit(Version2, UpdateOperations2, AuthorID, Client),

    % Search at the latest version - should find only one instance of the object
    LatestSearchRequest = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version3,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = LatestResults,
        total_count = LatestCount,
        continuation_token = LatestToken
    }} = dmt_client:search_objects(LatestSearchRequest, Client),

    % Verify only one result is returned (deduplication)
    ?assertEqual(1, LatestCount, "Should find exactly one object despite multiple updates"),
    ?assertEqual(1, length(LatestResults), "Should return exactly one result"),
    ?assertEqual(undefined, LatestToken, "Should not have continuation token"),

    % Verify the result contains the latest version's data
    [
        #domain_conf_v2_LimitedVersionedObject{
            ref = {category, ResultRef},
            info = #domain_conf_v2_VersionedObjectInfo{
                version = ResultVersion
            },
            name = ResultName,
            description = ResultDesc
        }
    ] = LatestResults,

    ?assertEqual(CategoryRef, ResultRef, "Should return the correct object reference"),
    ?assertEqual(Version3, ResultVersion, "Should return the object with the latest version"),
    ?assertEqual(UpdatedName2, ResultName, "Should return the latest name"),
    ?assertEqual(UpdatedDesc2, ResultDesc, "Should return the latest description"),

    % Verify searching at earlier versions returns the object as it existed then
    % Search at Version1
    V1SearchRequest = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version1,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = V1Results,
        total_count = V1Count
    }} = dmt_client:search_objects(V1SearchRequest, Client),

    ?assertEqual(1, V1Count, "Should find one object at version 1"),
    [
        #domain_conf_v2_LimitedVersionedObject{
            info = #domain_conf_v2_VersionedObjectInfo{
                version = V1Version
            },
            name = V1Name,
            description = V1Desc
        }
    ] = V1Results,

    ?assertEqual(Version1, V1Version, "Should return version 1 info"),
    ?assertEqual(InitialName, V1Name, "Should return the initial name at version 1"),
    ?assertEqual(InitialDesc, V1Desc, "Should return the initial description at version 1"),

    % Search at Version2
    V2SearchRequest = #domain_conf_v2_SearchRequestParams{
        query = SearchTerm,
        version = Version2,
        limit = 10
    },

    {ok, #domain_conf_v2_SearchResponse{
        result = V2Results,
        total_count = V2Count
    }} = dmt_client:search_objects(V2SearchRequest, Client),

    ?assertEqual(1, V2Count, "Should find one object at version 2"),
    [
        #domain_conf_v2_LimitedVersionedObject{
            info = #domain_conf_v2_VersionedObjectInfo{
                version = V2Version
            },
            name = V2Name,
            description = V2Desc
        }
    ] = V2Results,

    ?assertEqual(Version2, V2Version, "Should return version 2 info"),
    ?assertEqual(UpdatedName1, V2Name, "Should return the first updated name at version 2"),
    ?assertEqual(UpdatedDesc1, V2Desc, "Should return the first updated description at version 2"),

    % Test the same behavior with full objects search
    {ok, #domain_conf_v2_SearchFullResponse{
        result = FullResults,
        total_count = FullCount
    }} = dmt_client:search_full_objects(LatestSearchRequest, Client),

    ?assertEqual(1, FullCount, "Full search should also find exactly one object"),
    ?assertEqual(1, length(FullResults), "Full search should return exactly one result"),

    % Verify the full object contains the latest data
    [
        #domain_conf_v2_VersionedObject{
            info = #domain_conf_v2_VersionedObjectInfo{
                version = FullVersion
            },
            object =
                {category, #domain_CategoryObject{
                    ref = FullRef,
                    data = #domain_Category{
                        name = FullName,
                        description = FullDesc
                    }
                }}
        }
    ] = FullResults,

    ?assertEqual(Version3, FullVersion, "Full object should have the latest version"),
    ?assertEqual(CategoryRef, FullRef, "Full object should have the correct reference"),
    ?assertEqual(UpdatedName2, FullName, "Full object should have the latest name"),
    ?assertEqual(UpdatedDesc2, FullDesc, "Full object should have the latest description").

%% Helper function
create_author(Email, Client) ->
    AuthorParams = #domain_conf_v2_AuthorParams{
        email = Email,
        name = <<"Test Author">>
    },
    {ok, #domain_conf_v2_Author{id = AuthorID}} = dmt_client:create_author(AuthorParams, Client),
    AuthorID.
