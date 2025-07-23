-module(dmt_test_consumer).

-include_lib("brod/include/brod.hrl").
-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

-behaviour(gen_server).
-behaviour(brod_group_subscriber).

%% API
-export([
    start_link/0,
    start_link/1,
    stop/1,
    get_events/1,
    clear_events/1,
    wait_for_events/2,
    wait_for_events/3
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

%% brod_group_subscriber callbacks
-export([
    init/2,
    handle_message/4
]).

-define(DEFAULT_BROKERS, [{"kafka", 29092}]).
-define(DEFAULT_TOPIC, <<"domain_changes">>).
-define(CLIENT_ID, dmt_test_consumer_client).
-define(GROUP_ID, <<"dmt_test_consumer_group">>).

-type historical_commit() :: term().

-record(state, {
    client_id :: atom(),
    topic :: binary(),
    group_id :: binary(),
    events = [] :: [historical_commit()],
    subscriber_pid :: pid() | undefined
}).

%%====================================================================
%% API functions
%%====================================================================

%% @doc Start consumer with default configuration
-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    start_link(#{}).

%% @doc Start consumer with custom configuration
-spec start_link(map()) -> {ok, pid()} | {error, term()}.
start_link(Config) ->
    gen_server:start_link(?MODULE, Config, []).

%% @doc Stop the consumer
-spec stop(pid()) -> ok.
stop(Pid) ->
    gen_server:call(Pid, stop).

%% @doc Get collected events
-spec get_events(pid()) -> [historical_commit()].
get_events(Pid) ->
    gen_server:call(Pid, get_events).

%% @doc Clear collected events
-spec clear_events(pid()) -> ok.
clear_events(Pid) ->
    gen_server:call(Pid, clear_events).

%% @doc Wait for at least N events to be collected
-spec wait_for_events(pid(), non_neg_integer()) -> [historical_commit()].
wait_for_events(Pid, Count) ->
    wait_for_events(Pid, Count, 5000).

%% @doc Wait for at least N events to be collected with timeout
-spec wait_for_events(pid(), non_neg_integer(), timeout()) -> [historical_commit()].
wait_for_events(Pid, Count, Timeout) ->
    EndTime = erlang:system_time(millisecond) + Timeout,
    wait_for_events_loop(Pid, Count, EndTime).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init(Config) ->
    ClientId = maps:get(client_id, Config, ?CLIENT_ID),
    Topic = maps:get(topic, Config, ?DEFAULT_TOPIC),
    GroupId = maps:get(group_id, Config, ?GROUP_ID),
    Brokers = maps:get(brokers, Config, ?DEFAULT_BROKERS),

    % Start brod client
    case brod:start_client(Brokers, ClientId, []) of
        ok ->
            % Start group subscriber with this process as the callback module
            ConsumerConfig = [
                % Start from latest to avoid old messages
                {begin_offset, latest},
                {offset_commit_policy, commit_to_kafka_v2},
                {offset_commit_interval_seconds, 1}
            ],
            GroupConfig = [
                {offset_commit_policy, commit_to_kafka_v2},
                {offset_commit_interval_seconds, 1}
            ],

            case
                brod:start_link_group_subscriber(
                    ClientId,
                    GroupId,
                    [Topic],
                    GroupConfig,
                    ConsumerConfig,
                    ?MODULE,
                    self()
                )
            of
                {ok, SubscriberPid} ->
                    logger:info("Started Kafka test consumer for topic ~s", [Topic]),
                    {ok, #state{
                        client_id = ClientId,
                        topic = Topic,
                        group_id = GroupId,
                        subscriber_pid = SubscriberPid
                    }};
                {error, Reason} ->
                    logger:error("Failed to start group subscriber: ~p", [Reason]),
                    brod:stop_client(ClientId),
                    {stop, {subscriber_start_failed, Reason}}
            end;
        {error, Reason} ->
            logger:error("Failed to start brod client: ~p", [Reason]),
            {stop, {client_start_failed, Reason}}
    end.

handle_call(get_events, _From, #state{events = Events} = State) ->
    {reply, lists:reverse(Events), State};
handle_call(clear_events, _From, State) ->
    {reply, ok, State#state{events = []}};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({kafka_event, HistoricalCommit}, #state{events = Events} = State) ->
    NewEvents = [HistoricalCommit | Events],
    {noreply, State#state{events = NewEvents}};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(Info, State) ->
    logger:debug("Received unexpected info: ~p", [Info]),
    {noreply, State}.

terminate(_Reason, #state{client_id = ClientId, subscriber_pid = SubscriberPid}) ->
    % Stop subscriber if it's running
    case SubscriberPid of
        undefined ->
            ok;
        Pid when is_pid(Pid) ->
            catch gen_server:stop(Pid)
    end,
    % Stop brod client
    catch brod:stop_client(ClientId),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal functions
%%====================================================================

%% @doc Wait for events in a loop with timeout
wait_for_events_loop(Pid, Count, EndTime) ->
    case erlang:system_time(millisecond) of
        Now when Now >= EndTime ->
            Events = get_events(Pid),
            case length(Events) of
                N when N >= Count -> Events;
                N -> error({timeout_waiting_for_events, {expected, Count}, {got, N}})
            end;
        _Now ->
            Events = get_events(Pid),
            case length(Events) of
                N when N >= Count -> Events;
                _N ->
                    timer:sleep(100),
                    wait_for_events_loop(Pid, Count, EndTime)
            end
    end.

%% @doc Deserialize binary thrift data to HistoricalCommit record
-spec deserialize_historical_commit(binary()) -> {ok, term()} | {error, term()}.
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

%%====================================================================
%% brod_group_subscriber behaviour callbacks
%%====================================================================

%% @doc Initialize the group subscriber
init(_GroupId, ConsumerPid) ->
    logger:debug("Group subscriber init: GroupId=~p, ConsumerPid=~p", [_GroupId, ConsumerPid]),
    {ok, ConsumerPid}.

%% @doc Handle individual messages from brod
handle_message(Topic, Partition, Message, ConsumerPid) ->
    try
        #kafka_message{
            offset = Offset,
            key = Key,
            value = Value
        } = Message,

        logger:debug(
            "Received Kafka message: topic=~s, partition=~p, offset=~p, key=~s",
            [Topic, Partition, Offset, Key]
        ),

        % Deserialize the thrift message
        case deserialize_historical_commit(Value) of
            {ok, HistoricalCommit} ->
                logger:debug(
                    "Successfully deserialized HistoricalCommit: version=~p",
                    [HistoricalCommit#domain_conf_v2_HistoricalCommit.version]
                ),
                % Send the event to the gen_server
                gen_server:cast(ConsumerPid, {kafka_event, HistoricalCommit}),
                {ok, ack, ConsumerPid};
            {error, Reason} ->
                logger:error("Failed to deserialize HistoricalCommit: ~p", [Reason]),
                % Still ack to avoid reprocessing
                {ok, ack, ConsumerPid}
        end
    catch
        Class:Error:Stacktrace ->
            logger:error(
                "Exception processing Kafka message: ~p:~p~n~p",
                [Class, Error, Stacktrace]
            ),
            % Still ack to avoid reprocessing
            {ok, ack, ConsumerPid}
    end.
