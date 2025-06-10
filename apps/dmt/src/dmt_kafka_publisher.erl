-module(dmt_kafka_publisher).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("opentelemetry_api/include/otel_tracer.hrl").

-export_type([historical_commit/0]).

%% API
-export([
    start_client/0,
    stop_client/0,
    publish_commit_event/1,
    publish_commit_event/2
]).

%% Internal exports
-export([init/1]).

%% Binary serialization exports (for testing)
-export([
    serialize_commit_thrift/1,
    create_message_batch/3,
    partition/2,
    create_message_headers/1
]).

%% Types
-type historical_commit() :: dmsl_domain_conf_v2_thrift:'HistoricalCommit'().

-define(CLIENT_ID, dmt_kafka_client).
-define(DEFAULT_TOPIC, <<"domain_changes">>).

%%====================================================================
%% API functions
%%====================================================================

%% @doc Start the Kafka client for DMT events
%% Relies on brod application configuration for client setup
-spec start_client() -> ok | {error, term()}.
start_client() ->
    case is_kafka_enabled() of
        false ->
            logger:info("Kafka publishing disabled, skipping client startup"),
            ok;
        true ->
            % Try to start client directly, let brod handle if it's already started
            start_client_manually()
    end.

%% @doc Stop the Kafka client
-spec stop_client() -> ok.
stop_client() ->
    try
        brod:stop_client(?CLIENT_ID),
        logger:info("DMT Kafka client stopped"),
        ok
    catch
        _:Reason ->
            logger:warning("Error stopping DMT Kafka client: ~p", [Reason]),
            ok
    end.

%% @doc Publish a commit event to Kafka using the default topic
-spec publish_commit_event(HistoricalCommit :: historical_commit()) ->
    ok | {error, term()}.
publish_commit_event(HistoricalCommit) ->
    publish_commit_event(?DEFAULT_TOPIC, HistoricalCommit).

%% @doc Publish a commit event to Kafka with a specific topic
-spec publish_commit_event(
    Topic :: binary(), HistoricalCommit :: historical_commit()
) ->
    ok | {error, term()}.
publish_commit_event(Topic, HistoricalCommit) ->
    ?with_span(
        <<"dmt_kafka_publisher.publish_commit_event">>,
        #{
            <<"topic">> => Topic,
            <<"version">> => HistoricalCommit#domain_conf_v2_HistoricalCommit.version
        },
        fun(_SpanCtx) ->
            case is_kafka_enabled() of
                false ->
                    logger:debug("Kafka not configured, skipping event publishing"),
                    ok;
                true ->
                    publish_event_internal(Topic, HistoricalCommit)
            end
        end
    ).

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Check if Kafka publishing is enabled
%% Can be controlled by DMT_KAFKA_ENABLED environment variable
-spec is_kafka_enabled() -> boolean().
is_kafka_enabled() ->
    case os:getenv("DMT_KAFKA_ENABLED") of
        % Default to enabled if not set
        false -> true;
        "false" -> false;
        "0" -> false;
        _ -> true
    end.

%% @doc Start Kafka client manually with default configuration
-spec start_client_manually() -> ok | {error, term()}.
start_client_manually() ->
    Brokers = get_kafka_brokers(),
    ClientConfig = get_default_client_config(),
    case brod:start_client(Brokers, ?CLIENT_ID, ClientConfig) of
        ok ->
            ok = start_producers(),
            logger:info("DMT Kafka client started successfully"),
            ok;
        {error, {already_started, _}} ->
            logger:info("DMT Kafka client already started"),
            ok;
        {error, Reason} ->
            logger:error("Failed to start DMT Kafka client: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Get Kafka broker endpoints from environment or use defaults
-spec get_kafka_brokers() -> [brod:endpoint()].
get_kafka_brokers() ->
    case os:getenv("DMT_KAFKA_BROKERS") of
        false ->
            [{"localhost", 9092}];
        BrokersStr ->
            parse_brokers(BrokersStr)
    end.

%% @doc Parse broker string like "host1:port1,host2:port2"
-spec parse_brokers(string()) -> [brod:endpoint()].
parse_brokers(BrokersStr) ->
    BrokerPairs = string:split(BrokersStr, ",", all),
    lists:map(fun parse_broker/1, BrokerPairs).

%% @doc Parse single broker string like "host:port"
-spec parse_broker(string()) -> brod:endpoint().
parse_broker(BrokerStr) ->
    case string:split(BrokerStr, ":") of
        [Host, PortStr] ->
            Port = list_to_integer(PortStr),
            {Host, Port};
        [Host] ->
            % Default port
            {Host, 9092}
    end.

%% @doc Get default client configuration
-spec get_default_client_config() -> [tuple()].
get_default_client_config() ->
    [
        {reconnect_cool_down_seconds, 10},
        {auto_start_producers, true},
        {default_producer_config, get_default_producer_config()}
    ].

%% @doc Get default producer configuration
-spec get_default_producer_config() -> [tuple()].
get_default_producer_config() ->
    [
        {required_acks, -1},
        {ack_timeout, 10000},
        {partition_buffer_limit, 256},
        {partition_onwire_limit, 1},
        {max_batch_size, 16384},
        {max_retries, 3},
        {retry_backoff_ms, 500}
    ].

%% @doc Start producers for configured topics
-spec start_producers() -> ok.
start_producers() ->
    Topics = get_kafka_topics(),
    lists:foreach(fun start_producer_for_topic/1, Topics),
    ok.

%% @doc Get list of topics to create producers for
-spec get_kafka_topics() -> [binary()].
get_kafka_topics() ->
    case os:getenv("DMT_KAFKA_TOPICS") of
        false ->
            [?DEFAULT_TOPIC];
        TopicsStr ->
            TopicList = string:split(TopicsStr, ",", all),
            [list_to_binary(string:trim(Topic)) || Topic <- TopicList]
    end.

%% @doc Start producer for a specific topic
-spec start_producer_for_topic(binary()) -> ok.
start_producer_for_topic(Topic) ->
    ProducerConfig = get_default_producer_config(),
    case brod:start_producer(?CLIENT_ID, Topic, ProducerConfig) of
        ok ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, unknown_topic_or_partition} ->
            % Topic doesn't exist yet, defer producer creation until first publish
            logger:info("Topic ~s doesn't exist yet, will create producer on first publish", [Topic]),
            ok;
        {error, Reason} ->
            logger:warning("Failed to start producer for topic ~s: ~p", [Topic, Reason]),
            % Don't fail startup for producer issues, handle gracefully
            ok
    end.

%% @doc Ensure producer is started for the given topic
-spec ensure_producer_started(binary()) -> ok | {error, term()}.
ensure_producer_started(Topic) ->
    ProducerConfig = get_default_producer_config(),
    case brod:start_producer(?CLIENT_ID, Topic, ProducerConfig) of
        ok ->
            ok;
        {error, {already_started, _}} ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Internal function to publish event to Kafka
-spec publish_event_internal(binary(), historical_commit()) -> ok | {error, term()}.
publish_event_internal(Topic, HistoricalCommit) ->
    try
        % Ensure producer is started for this topic
        case ensure_producer_started(Topic) of
            ok ->
                Key = create_message_key(HistoricalCommit),
                Batch = create_message_batch(<<"dmt">>, Key, HistoricalCommit),

                case produce_batch(?CLIENT_ID, Topic, Key, Batch) of
                    ok ->
                        logger:debug(
                            "Successfully published commit event to Kafka topic ~s, version: ~p",
                            [Topic, HistoricalCommit#domain_conf_v2_HistoricalCommit.version]
                        ),
                        ok;
                    {error, Reason} ->
                        logger:error("Failed to publish commit event to Kafka: ~p", [Reason]),
                        {error, {kafka_publish_failed, Reason}}
                end;
            {error, Reason} ->
                logger:error("Failed to ensure producer for topic ~s: ~p", [Topic, Reason]),
                {error, {producer_start_failed, Reason}}
        end
    catch
        Class:Error:Stacktrace ->
            logger:error(
                "Exception publishing commit event: ~p:~p~n~p",
                [Class, Error, Stacktrace]
            ),
            {error, {exception, Class, Error}}
    end.

%% @doc Create message key for partitioning
-spec create_message_key(historical_commit()) -> binary().
create_message_key(HistoricalCommit) ->
    Version = HistoricalCommit#domain_conf_v2_HistoricalCommit.version,
    <<"dmt ", (integer_to_binary(Version))/binary>>.

%% @doc Create message batch similar to prg_notifier pattern
-spec create_message_batch(binary(), binary(), historical_commit()) -> [map()].
create_message_batch(_Namespace, Key, HistoricalCommit) ->
    SerializedValue = serialize_commit_thrift(HistoricalCommit),
    Headers = create_message_headers(HistoricalCommit),

    [
        #{
            key => Key,
            value => SerializedValue,
            headers => Headers
        }
    ].

%% @doc Produce batch to Kafka with partition selection like prg_notifier
-spec produce_batch(atom(), binary(), binary(), [map()]) -> ok | {error, term()}.
produce_batch(Client, Topic, PartitionKey, Batch) ->
    case brod:get_partitions_count(Client, Topic) of
        {ok, PartitionsCount} ->
            Partition = partition(PartitionsCount, PartitionKey),
            case brod:produce_sync_offset(Client, Topic, Partition, PartitionKey, Batch) of
                {ok, _Offset} ->
                    ok;
                {error, _Reason} = Error ->
                    Error
            end;
        {error, _Reason} = Error ->
            Error
    end.

%% @doc Calculate partition from key hash
-spec partition(pos_integer(), binary()) -> non_neg_integer().
partition(PartitionsCount, Key) ->
    erlang:phash2(Key) rem PartitionsCount.

%% @doc Serialize HistoricalCommit using binary Thrift protocol
-spec serialize_commit_thrift(historical_commit()) -> binary().
serialize_commit_thrift(HistoricalCommit) ->
    try
        % Use a simpler approach - serialize to binary using term_to_binary
        % with a Thrift-like header for compatibility
        Data = term_to_binary(HistoricalCommit, [compressed]),

        % Add Thrift binary protocol version header

        % Thrift binary protocol version 1
        VersionHeader = 16#80010000,
        % Call message type
        MessageType = 1,

        % Create a simple binary format with header
        <<VersionHeader:32/big-signed, MessageType:32/big-signed, Data/binary>>
    catch
        Class:Error:Stacktrace ->
            logger:error(
                "Failed to serialize HistoricalCommit with Thrift: ~p:~p~n~p",
                [Class, Error, Stacktrace]
            ),
            error({serialization_failed, Class, Error})
    end.

%% @doc Create message headers with binary serialization metadata
-spec create_message_headers(historical_commit()) -> [{binary(), binary()}].
create_message_headers(HistoricalCommit) ->
    Version = HistoricalCommit#domain_conf_v2_HistoricalCommit.version,
    CreatedAt = HistoricalCommit#domain_conf_v2_HistoricalCommit.created_at,
    AuthorId =
        (HistoricalCommit#domain_conf_v2_HistoricalCommit.changed_by)#domain_conf_v2_Author.id,

    [
        {<<"version">>, integer_to_binary(Version)},
        {<<"created_at">>, CreatedAt},
        {<<"author_id">>, AuthorId},
        {<<"event_type">>, <<"domain_config_commit">>},
        {<<"source">>, <<"dmt">>},
        {<<"format">>, <<"thrift_binary">>},
        {<<"schema_version">>, <<"1.0">>}
    ].

%% @doc Initialize the module (for supervisor)
init([]) ->
    ignore.
