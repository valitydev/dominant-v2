-module(dmt_kafka_publisher).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").
-include_lib("opentelemetry_api/include/otel_tracer.hrl").

-export_type([historical_commit/0]).

%% API
-export([
    publish_commit_event/1,
    publish_commit_event/2
]).

%% Types
-type historical_commit() :: dmsl_domain_conf_v2_thrift:'HistoricalCommit'().

-define(CLIENT_ID, dmt_kafka_client).
-define(DEFAULT_TOPIC, <<"domain_changes">>).

%%====================================================================
%% API functions
%%====================================================================

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
    case is_kafka_enabled() of
        false ->
            logger:debug("Kafka not configured, skipping event publishing"),
            ok;
        true ->
            publish_event_internal(Topic, HistoricalCommit)
    end.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Check if Kafka publishing is enabled
%% Reads from DMT application configuration
-spec is_kafka_enabled() -> boolean().
is_kafka_enabled() ->
    case application:get_env(dmt, kafka) of
        {ok, #{enabled := Enabled}} when is_boolean(Enabled) ->
            Enabled;
        _ ->
            false
    end.

%% @doc Get the configured Kafka topic
-spec get_kafka_topic() -> binary().
get_kafka_topic() ->
    case application:get_env(dmt, kafka) of
        {ok, #{topic := Topic}} ->
            Topic;
        undefined ->
            ?DEFAULT_TOPIC
    end.

%% @doc Internal function to publish event to Kafka
-spec publish_event_internal(binary(), historical_commit()) -> ok | {error, term()}.
publish_event_internal(Topic, HistoricalCommit) ->
    try
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

    [
        #{
            key => Key,
            value => SerializedValue
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
