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
            % Use the brod-configured client directly
            start_client_from_brod_config()
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

%% @doc Publish a commit event to Kafka using the configured topic
-spec publish_commit_event(HistoricalCommit :: historical_commit()) ->
    ok | {error, term()}.
publish_commit_event(HistoricalCommit) ->
    Topic = get_kafka_topic(),
    publish_commit_event(Topic, HistoricalCommit).

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
%% Reads from DMT application configuration
-spec is_kafka_enabled() -> boolean().
is_kafka_enabled() ->
    case application:get_env(dmt, kafka) of
        {ok, KafkaConfig} when is_map(KafkaConfig) ->
            maps:get(enabled, KafkaConfig, false);
        {ok, KafkaConfig} when is_list(KafkaConfig) ->
            % Backwards compatibility with proplist
            proplists:get_value(enabled, KafkaConfig, false);
        undefined ->
            false
    end.

%% @doc Start Kafka client using brod application configuration
-spec start_client_from_brod_config() -> ok | {error, term()}.
start_client_from_brod_config() ->
    % Try to start the client using brod configuration
    case get_brod_client_config() of
        {ok, {Endpoints, ClientConfig}} ->
            case brod:start_client(Endpoints, ?CLIENT_ID, ClientConfig) of
                ok ->
                    logger:info("DMT Kafka client started from brod config"),
                    ok = start_producers(),
                    ok;
                {error, {already_started, _}} ->
                    logger:info("DMT Kafka client already started"),
                    ok = start_producers(),
                    ok;
                {error, Reason} ->
                    logger:error("Failed to start DMT Kafka client: ~p", [Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            logger:error("Failed to get brod client configuration: ~p", [Reason]),
            {error, Reason}
    end.

%% @doc Get brod client configuration from application environment
-spec get_brod_client_config() -> {ok, {[brod:endpoint()], [tuple()]}} | {error, term()}.
get_brod_client_config() ->
    case application:get_env(brod, clients) of
        {ok, Clients} ->
            case proplists:get_value(?CLIENT_ID, Clients) of
                undefined ->
                    {error, {client_not_configured, ?CLIENT_ID}};
                ClientConfig ->
                    case proplists:get_value(endpoints, ClientConfig) of
                        undefined ->
                            {error, {endpoints_not_configured, ?CLIENT_ID}};
                        Endpoints ->
                            {ok, {Endpoints, ClientConfig}}
                    end
            end;
        undefined ->
            {error, brod_clients_not_configured}
    end.

%% @doc Get producer configuration from brod client config or defaults
-spec get_producer_config() -> [tuple()].
get_producer_config() ->
    case get_brod_client_config() of
        {ok, {_, ClientConfig}} ->
            proplists:get_value(
                default_producer_config, ClientConfig, get_default_producer_config()
            );
        {error, _} ->
            get_default_producer_config()
    end.

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

%% @doc Get the configured Kafka topic
-spec get_kafka_topic() -> binary().
get_kafka_topic() ->
    case application:get_env(dmt, kafka) of
        {ok, KafkaConfig} when is_map(KafkaConfig) ->
            maps:get(topic, KafkaConfig, ?DEFAULT_TOPIC);
        {ok, KafkaConfig} when is_list(KafkaConfig) ->
            % Backwards compatibility with proplist
            proplists:get_value(topic, KafkaConfig, ?DEFAULT_TOPIC);
        undefined ->
            ?DEFAULT_TOPIC
    end.

%% @doc Get list of topics to create producers for (just the main topic now)
-spec get_kafka_topics() -> [binary()].
get_kafka_topics() ->
    [get_kafka_topic()].

%% @doc Start producer for a specific topic
-spec start_producer_for_topic(binary()) -> ok.
start_producer_for_topic(Topic) ->
    ProducerConfig = get_producer_config(),
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
    ProducerConfig = get_producer_config(),
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

%% @doc Serialize HistoricalCommit using proper Thrift binary protocol
-spec serialize_commit_thrift(historical_commit()) -> binary().
serialize_commit_thrift(HistoricalCommit) ->
    try
        Codec = thrift_strict_binary_codec:new(),
        Type = {struct, struct, {dmsl_domain_conf_v2_thrift, 'HistoricalCommit'}},
        case thrift_strict_binary_codec:write(Codec, Type, HistoricalCommit) of
            {ok, NewCodec} ->
                thrift_strict_binary_codec:close(NewCodec);
            {error, Reason} ->
                logger:error("Failed to serialize HistoricalCommit with Thrift: ~p", [Reason]),
                error({thrift_serialization_failed, Reason})
        end
    catch
        Class:Error:Stacktrace ->
            logger:error(
                "Exception during HistoricalCommit serialization: ~p:~p~n~p",
                [Class, Error, Stacktrace]
            ),
            error({serialization_exception, Class, Error})
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
