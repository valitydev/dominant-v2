-module(dmt_kafka_integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
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
-export([test_end_to_end_workflow/1]).

-define(CLIENT_ID, dmt_kafka_test_client).

%%====================================================================
%% CT Callbacks
%%====================================================================

all() ->
    [
        test_end_to_end_workflow
    ].

init_per_suite(Config) ->
    {Apps, _} = dmt_ct_helper:start_apps([woody, scoper, epg_connector, brod, dmt]),

    _ = dmt_ct_helper:create_kafka_topics(),
    ApiClient = dmt_ct_helper:create_client(),
    [{client, ApiClient}, {apps, Apps} | Config].

end_per_suite(_Config) ->
    dmt_ct_helper:cleanup_db(),

    ok.

init_per_testcase(test_end_to_end_workflow, Config) ->
    %% Start the test consumer to collect events
    {ok, ConsumerPid} = dmt_test_consumer:start_link(),
    [{consumer_pid, ConsumerPid} | Config];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(test_end_to_end_workflow, Config) ->
    ok = dmt_test_consumer:stop(dmt_ct_helper:cfg(consumer_pid, Config)),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_end_to_end_workflow(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),
    ConsumerPid = dmt_ct_helper:cfg(consumer_pid, Config),

    %% Clear any existing events
    ok = dmt_test_consumer:clear_events(ConsumerPid),

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

    {ok, CommitResponse} = dmt_client:commit(Revision, Operations, AuthorID, Client),

    %% Wait for the Kafka event to be published and consumed
    Events = dmt_test_consumer:wait_for_events(ConsumerPid, 1, 10000),

    %% Validate that we received exactly one event
    ?assertEqual(1, length(Events)),

    [HistoricalCommit] = Events,

    %% Validate the event content
    #domain_conf_v2_HistoricalCommit{
        version = EventVersion,
        ops = EventOps,
        created_at = CreatedAt,
        changed_by = EventAuthor
    } = HistoricalCommit,

    %% Check that the version in the event matches the commit response
    #domain_conf_v2_CommitResponse{version = ResponseVersion} = CommitResponse,
    ?assertEqual(ResponseVersion, EventVersion),

    %% Check that the author matches
    #domain_conf_v2_Author{id = EventAuthorID} = EventAuthor,
    ?assertEqual(AuthorID, EventAuthorID),

    %% Check that created_at is set
    ?assertNotEqual(undefined, CreatedAt),

    ?assertMatch(
        [
            {insert, #domain_conf_v2_FinalInsertOp{
                object = {category, #domain_CategoryObject{ref = CategoryRef, data = Category}}
            }}
        ],
        EventOps
    ).

%%====================================================================
%% Helper Functions
%%====================================================================

create_author(Email, Client) ->
    AuthorParams = #domain_conf_v2_AuthorParams{
        email = Email,
        name = <<"some_name">>
    },

    {ok, #domain_conf_v2_Author{
        id = AuthorID
    }} = dmt_client:create_author(AuthorParams, Client),
    AuthorID.
