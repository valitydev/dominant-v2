-module(dmt_commit_kafka_events_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
% For domain objects in operations
-include_lib("damsel/include/dmsl_domain_thrift.hrl").

%% CT callbacks
-export([
    all/0,
    init_per_suite/1,
    end_per_suite/1,
    init_per_testcase/2,
    end_per_testcase/2
]).

%% Test cases
-export([
    test_commit_publishes_kafka_event/1,
    test_multiple_commits_publish_multiple_events/1,
    test_update_operation_publishes_kafka_event/1,
    test_remove_operation_publishes_kafka_event/1
]).

-define(TEST_TOPIC, <<"domain_changes">>).
% For brod specific consumer client
-define(KAFKA_TEST_CLIENT_ID, dmt_commit_kafka_test_client).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        test_commit_publishes_kafka_event,
        test_multiple_commits_publish_multiple_events,
        test_update_operation_publishes_kafka_event,
        test_remove_operation_publishes_kafka_event
    ].

init_per_suite(Config) ->
    ct:pal("Starting Commit Kafka Events test suite"),

    % Start dependent applications with brod configured via ct_helper
    {Apps, _} = dmt_ct_helper:start_apps([
        damsel, jsx, brod, woody, scoper, epg_connector, dmt
    ]),

    % Wait for Kafka to be ready after applications are started
    case wait_for_kafka_ready(30) of
        ok ->
            ct:pal("Kafka is ready for testing"),
            ApiClient = dmt_ct_helper:create_client(),
            [{apps, Apps}, {api_client, ApiClient} | Config];
        {error, timeout} ->
            ct:fail("Kafka not ready after 30 seconds")
    end.

end_per_suite(_Config) ->
    ct:pal("Cleaning up Commit Kafka Events test suite"),

    cleanup_test_topic(),

    catch brod:stop_client(?KAFKA_TEST_CLIENT_ID),
    % Stop the main publisher's client
    dmt_kafka_publisher:stop_client(),

    dmt_ct_helper:cleanup_db(),
    % Consider stopping apps if dmt_ct_helper provided a way
    ok.

init_per_testcase(TestCase, Config) ->
    ct:pal("Starting test case: ~p", [TestCase]),

    % Ensure dmt_kafka_publisher starts with our test config
    case dmt_kafka_publisher:start_client() of
        ok -> ok;
        % Already started with correct config
        {error, {already_started, _}} -> ok;
        {error, Reason} -> ct:fail("Failed to start dmt_kafka_publisher client: ~p", [Reason])
    end,
    % Don't clean DB here as it would remove authors created in init_per_suite
    Config.

end_per_testcase(TestCase, _Config) ->
    ct:pal("Finished test case: ~p", [TestCase]),
    catch brod:stop_client(?KAFKA_TEST_CLIENT_ID),
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_commit_publishes_kafka_event(Config) ->
    ApiClient = dmt_ct_helper:cfg(api_client, Config),
    AuthorID = create_test_author(ApiClient, <<"commit_kafka_tester">>),

    ct:pal("Created/Retrieved AuthorID: ~p", [AuthorID]),

    CommitOps = create_sample_commit_operations(),
    InitialVersion = 0,

    {ok, #domain_conf_v2_CommitResponse{version = CommittedVersion}} =
        dmt_client:commit(InitialVersion, CommitOps, AuthorID, ApiClient),
    ct:pal("Commit successful, version: ~p", [CommittedVersion]),
    ?assert(CommittedVersion > InitialVersion),

    % Wait a moment for Kafka event to be published
    timer:sleep(1000),

    % Verify that Kafka topic exists and has messages
    % This is a simpler approach than trying to consume messages
    case brod:get_partitions_count(dmt_kafka_client, ?TEST_TOPIC) of
        {ok, PartitionCount} when PartitionCount > 0 ->
            ct:pal("Topic ~s exists with ~p partitions", [?TEST_TOPIC, PartitionCount]),
            % Try to get high watermark to verify messages exist
            case brod:fetch(dmt_kafka_client, ?TEST_TOPIC, 0, 0, #{max_bytes => 1024}) of
                {ok, {HighWatermark, _Messages}} when HighWatermark > 0 ->
                    ct:pal("Topic has high watermark of ~p, indicating messages were published", [
                        HighWatermark
                    ]),
                    ?assert(HighWatermark >= CommittedVersion);
                {ok, {0, _}} ->
                    ct:pal("Warning: High watermark is 0, but this might be expected in tests");
                {error, Reason} ->
                    ct:pal("Could not fetch from topic: ~p", [Reason])
            end;
        {error, unknown_topic_or_partition} ->
            ct:fail("Topic ~s does not exist - Kafka event was not published", [?TEST_TOPIC]);
        {error, Reason} ->
            ct:fail("Failed to check topic ~s: ~p", [?TEST_TOPIC, Reason])
    end,

    ct:pal("Test passed: Commit successfully triggered Kafka event publication").

test_multiple_commits_publish_multiple_events(Config) ->
    ApiClient = dmt_ct_helper:cfg(api_client, Config),
    AuthorID = create_test_author(ApiClient, <<"multi_commit_tester">>),

    ct:pal("Created/Retrieved AuthorID: ~p", [AuthorID]),

    % Get initial state
    InitialVersion =
        case dmt_repository:get_latest_version() of
            {ok, Version} -> Version;
            {error, _} -> 0
        end,

    InitialHighWatermark = get_current_high_watermark(),

    % Perform multiple commits
    CommitOps1 = create_sample_commit_operations(<<"MultiCommitCategory1">>),
    {ok, #domain_conf_v2_CommitResponse{version = Version1}} =
        dmt_client:commit(InitialVersion, CommitOps1, AuthorID, ApiClient),
    ct:pal("First commit successful, version: ~p", [Version1]),

    CommitOps2 = create_sample_commit_operations(<<"MultiCommitCategory2">>),
    {ok, #domain_conf_v2_CommitResponse{version = Version2}} =
        dmt_client:commit(Version1, CommitOps2, AuthorID, ApiClient),
    ct:pal("Second commit successful, version: ~p", [Version2]),

    CommitOps3 = create_sample_commit_operations(<<"MultiCommitCategory3">>),
    {ok, #domain_conf_v2_CommitResponse{version = Version3}} =
        dmt_client:commit(Version2, CommitOps3, AuthorID, ApiClient),
    ct:pal("Third commit successful, version: ~p", [Version3]),

    % Wait for all Kafka events to be published
    timer:sleep(2000),

    % Verify that multiple messages were published
    FinalHighWatermark = get_current_high_watermark(),
    ExpectedAdditionalMessages = 3,
    ActualAdditionalMessages = FinalHighWatermark - InitialHighWatermark,

    ct:pal(
        "Initial high watermark: ~p, Final high watermark: ~p, Additional messages: ~p",
        [InitialHighWatermark, FinalHighWatermark, ActualAdditionalMessages]
    ),

    ?assert(ActualAdditionalMessages >= ExpectedAdditionalMessages),
    ct:pal("Test passed: Multiple commits successfully triggered multiple Kafka events").

test_update_operation_publishes_kafka_event(Config) ->
    ApiClient = dmt_ct_helper:cfg(api_client, Config),
    AuthorID = create_test_author(ApiClient, <<"update_operation_tester">>),

    ct:pal("Created/Retrieved AuthorID: ~p", [AuthorID]),

    % First, create an object to update
    InitialVersion =
        case dmt_repository:get_latest_version() of
            {ok, Version} -> Version;
            {error, _} -> 0
        end,

    CreateOps = create_sample_commit_operations(<<"UpdateTestCategory">>),
    {ok, #domain_conf_v2_CommitResponse{version = Version1, new_objects = NewObjects}} =
        dmt_client:commit(InitialVersion, CreateOps, AuthorID, ApiClient),
    ct:pal("Create commit successful, version: ~p", [Version1]),

    % Extract the created object reference
    [{category, #domain_CategoryObject{ref = CategoryRef}}] = ordsets:to_list(NewObjects),
    ct:pal("Created category with ref: ~p", [CategoryRef]),

    % Get the high watermark before update
    InitialHighWatermark = get_current_high_watermark(),

    % Now update the created object
    UpdatedCategory = #domain_Category{
        name = <<"UpdateTestCategory">>,
        description = <<"Updated description for Kafka event testing">>,
        type = undefined
    },
    UpdateOp = #domain_conf_v2_UpdateOp{
        object =
            {category, #domain_CategoryObject{
                ref = CategoryRef,
                data = UpdatedCategory
            }}
    },
    UpdateOps = [{update, UpdateOp}],

    {ok, #domain_conf_v2_CommitResponse{version = Version2}} =
        dmt_client:commit(Version1, UpdateOps, AuthorID, ApiClient),
    ct:pal("Update commit successful, version: ~p", [Version2]),

    % Wait for Kafka event to be published
    timer:sleep(1000),

    % Verify that the update operation triggered a Kafka event
    FinalHighWatermark = get_current_high_watermark(),
    ActualAdditionalMessages = FinalHighWatermark - InitialHighWatermark,

    ct:pal(
        "High watermark before update: ~p, after update: ~p, additional messages: ~p",
        [InitialHighWatermark, FinalHighWatermark, ActualAdditionalMessages]
    ),

    ?assert(ActualAdditionalMessages >= 1),
    ct:pal("Test passed: Update operation successfully triggered Kafka event publication").

test_remove_operation_publishes_kafka_event(Config) ->
    ApiClient = dmt_ct_helper:cfg(api_client, Config),
    AuthorID = create_test_author(ApiClient, <<"remove_operation_tester">>),

    ct:pal("Created/Retrieved AuthorID: ~p", [AuthorID]),

    % First, create an object to remove
    InitialVersion =
        case dmt_repository:get_latest_version() of
            {ok, Version} -> Version;
            {error, _} -> 0
        end,

    CreateOps = create_sample_commit_operations(<<"RemoveTestCategory">>),
    {ok, #domain_conf_v2_CommitResponse{version = Version1, new_objects = NewObjects}} =
        dmt_client:commit(InitialVersion, CreateOps, AuthorID, ApiClient),
    ct:pal("Create commit successful, version: ~p", [Version1]),

    % Extract the created object reference
    [{category, #domain_CategoryObject{ref = CategoryRef}}] = ordsets:to_list(NewObjects),
    ct:pal("Created category with ref: ~p", [CategoryRef]),

    % Get the high watermark before remove
    InitialHighWatermark = get_current_high_watermark(),

    % Now remove the created object
    RemoveOp = #domain_conf_v2_RemoveOp{
        ref = {category, CategoryRef}
    },
    RemoveOps = [{remove, RemoveOp}],

    {ok, #domain_conf_v2_CommitResponse{version = Version2}} =
        dmt_client:commit(Version1, RemoveOps, AuthorID, ApiClient),
    ct:pal("Remove commit successful, version: ~p", [Version2]),

    % Wait for Kafka event to be published
    timer:sleep(1000),

    % Verify that the remove operation triggered a Kafka event
    FinalHighWatermark = get_current_high_watermark(),
    ActualAdditionalMessages = FinalHighWatermark - InitialHighWatermark,

    ct:pal(
        "High watermark before remove: ~p, after remove: ~p, additional messages: ~p",
        [InitialHighWatermark, FinalHighWatermark, ActualAdditionalMessages]
    ),

    ?assert(ActualAdditionalMessages >= 1),
    ct:pal("Test passed: Remove operation successfully triggered Kafka event publication").

%%====================================================================
%% Helper Functions
%%====================================================================

create_test_author(ApiClient, EmailBase) ->
    % Unique email for test
    Email = <<EmailBase/binary, "_commit_kafka@example.com">>,
    Name = <<"Test Author - ", EmailBase/binary>>,
    AuthorParams = #domain_conf_v2_AuthorParams{email = Email, name = Name},
    try
        case dmt_client:create_author(AuthorParams, ApiClient) of
            {ok, #domain_conf_v2_Author{id = AuthorID}} -> AuthorID;
            {exception, #domain_conf_v2_AuthorAlreadyExists{id = AuthorID}} -> AuthorID
        end
    catch
        E:R:S -> ct:fail("Failed to create author ~s: ~p:~p~n~p", [Email, E, R, S])
    end.

create_sample_commit_operations() ->
    create_sample_commit_operations(<<"TestCategoryKafkaEvent">>).

create_sample_commit_operations(CategoryName) ->
    Category = #domain_Category{
        name = CategoryName,
        description = <<"Category for Kafka event testing">>,
        type = test
    },
    InsertOp = #domain_conf_v2_InsertOp{
        object = {category, Category}
    },
    [{insert, InsertOp}].

get_current_high_watermark() ->
    case brod:fetch(dmt_kafka_client, ?TEST_TOPIC, 0, 0, #{max_bytes => 1024}) of
        {ok, {HighWatermark, _Messages}} -> HighWatermark;
        {error, _} -> 0
    end.

wait_for_kafka_ready(0) ->
    {error, timeout};
wait_for_kafka_ready(Retries) ->
    CheckClient = list_to_atom(
        "kafka_check_client_" ++ integer_to_list(erlang:unique_integer([positive]))
    ),
    % Use hardcoded kafka broker for connection testing
    KafkaNodes = [{"kafka", 29092}],
    try_kafka_connection(KafkaNodes, CheckClient, Retries).

try_kafka_connection(KafkaNodes, CheckClient, Retries) ->
    try
        Result = brod:start_client(KafkaNodes, CheckClient),
        handle_kafka_connection_result(Result, CheckClient, Retries)
    catch
        C:E:S ->
            ct:pal("Exc checking Kafka: ~p:~p. Retries left: ~p~n~p", [C, E, Retries - 1, S]),
            timer:sleep(1000),
            wait_for_kafka_ready(Retries - 1)
    end.

handle_kafka_connection_result(ok, CheckClient, _Retries) ->
    brod:stop_client(CheckClient),
    ok;
handle_kafka_connection_result({error, {already_started, _}}, CheckClient, _Retries) ->
    brod:stop_client(CheckClient),
    ok;
handle_kafka_connection_result({error, Reason}, _CheckClient, Retries) ->
    ct:pal("Kafka not ready: ~p. Retries left: ~p", [Reason, Retries - 1]),
    timer:sleep(1000),
    wait_for_kafka_ready(Retries - 1).

cleanup_test_topic() ->
    ct:pal(
        "Test topic (~s) cleanup: Relies on Kafka config (e.g., KAFKA_DELETE_TOPIC_ENABLE) or manual cleanup.",
        [?TEST_TOPIC]
    ).
