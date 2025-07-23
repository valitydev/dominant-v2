-module(dmt_kafka_integration_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("brod/include/brod.hrl").
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

-define(CLIENT_ID, dmt_kafka_client).

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

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    ok.

%%====================================================================
%% Test Cases
%%====================================================================

test_end_to_end_workflow(Config) ->
    Client = dmt_ct_helper:cfg(client, Config),

    {ok, {CurrentOffset, _}} = brod:fetch(?CLIENT_ID, <<"domain_changes">>, 0, 0),

    Email = <<"test_end_to_end_workflow">>,
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

    %% Fetch the event from Kafka
    {ok, {_NewOffset, Events}} = brod:fetch(?CLIENT_ID, <<"domain_changes">>, 0, CurrentOffset),

    %% Validate that we received exactly one event
    ?assertEqual(1, length(Events)),

    [HistoricalCommit] = [unmarshal_kafka_event_value(Event) || Event <- Events],

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

unmarshal_kafka_event_value(Event) ->
    #kafka_message{
        offset = _Offset,
        key = _Key,
        value = Value
    } = Event,
    {ok, HistoricalCommit} = deserialize_historical_commit(Value),
    HistoricalCommit.

deserialize_historical_commit(BinaryData) ->
    try
        Codec = thrift_strict_binary_codec:new(BinaryData),
        Type = {struct, struct, {dmsl_domain_conf_v2_thrift, 'HistoricalCommit'}},
        case thrift_strict_binary_codec:read(Codec, Type) of
            {ok, HistoricalCommit, _NewCodec} ->
                {ok, HistoricalCommit};
            {error, Reason} ->
                {error, {thrift_deserialization_failed, Reason}}
        end
    catch
        Class:Error:Stacktrace ->
            logger:error(
                "Exception during deserialization: ~p:~p~n~p",
                [Class, Error, Stacktrace]
            ),
            {error, {deserialization_exception, Class, Error}}
    end.
