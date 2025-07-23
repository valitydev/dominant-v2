-module(dmt_ct_helper).

-export([start_app/1]).
-export([start_app/2]).
-export([start_apps/1]).

-export([cfg/2]).

-export([create_client/0]).
-export([create_client/1]).

-export([cleanup_db/0]).
-export([create_kafka_topics/0]).
-export([delete_kafka_topics/0]).

-export_type([config/0]).
-export_type([test_case_name/0]).
-export_type([group_name/0]).

%%

-type app_name() :: atom().
-export_type([app_name/0]).

-define(BROKERS, [{"kafka", 29092}]).
-define(TEST_TOPIC, <<"domain_changes">>).

-spec start_app(app_name()) -> {[app_name()], map()}.
start_app(scoper = AppName) ->
    {
        start_app(AppName, [
            {storage, scoper_storage_logger}
        ]),
        #{}
    };
start_app(woody = AppName) ->
    {
        start_app(AppName, [
            {acceptors_pool_size, 4}
        ]),
        #{}
    };
start_app(dmt = AppName) ->
    {
        start_app(AppName, [
            {host, <<"dmt">>},
            {port, 8022},
            {scoper_event_handler_options, #{
                event_handler_opts => #{
                    formatter_opts => #{
                        max_length => 1000
                    }
                }
            }},
            {epg_db_name, dmt},
            {services, #{
                repository => #{
                    url => <<"http://dmt.default:8022/v1/domain/repository">>
                },
                repository_client => #{
                    url => <<"http://dmt.default:8022/v1/domain/repository_client">>
                },
                author => #{
                    url => <<"http://dmt.default:8022/v1/domain/author">>
                }
            }},
            {kafka, #{
                enabled => true,
                topic => ?TEST_TOPIC
            }}
        ]),
        #{}
    };
start_app(epg_connector = AppName) ->
    {
        start_app(AppName, [
            {databases, #{
                dmt => #{
                    host => "dmt_db",
                    port => 5432,
                    username => "postgres",
                    password => "postgres",
                    database => "dmt"
                }
            }},
            {pools, #{
                default_pool => #{
                    database => dmt,
                    size => 10
                },
                author_pool => #{
                    database => dmt,
                    size => 10
                }
            }}
        ]),
        #{}
    };
start_app(brod = AppName) ->
    {
        start_app(AppName, [
            {clients, [
                {dmt_kafka_client, [
                    {endpoints, ?BROKERS},
                    {reconnect_cool_down_seconds, 10},
                    {auto_start_producers, true},
                    {default_producer_config, []}
                ]}
            ]}
        ]),
        #{}
    };
start_app(AppName) ->
    {genlib_app:start_application(AppName), #{}}.

-spec start_app(app_name(), list()) -> [app_name()].
start_app(cowboy = AppName, Env) ->
    #{
        listener_ref := Ref,
        acceptors_count := Count,
        transport_opts := TransOpt,
        proto_opts := ProtoOpt
    } = Env,
    {ok, _} = cowboy:start_clear(Ref, [{num_acceptors, Count} | TransOpt], ProtoOpt),
    [AppName];
start_app(AppName, Env) ->
    genlib_app:start_application_with(AppName, Env).

-spec start_apps([app_name() | {app_name(), list()}]) -> {[app_name()], map()}.
start_apps(Apps) ->
    lists:foldl(
        fun
            ({AppName, Env}, {AppsAcc, RetAcc}) ->
                {lists:reverse(start_app(AppName, Env)) ++ AppsAcc, RetAcc};
            (AppName, {AppsAcc, RetAcc}) ->
                {Apps0, Ret0} = start_app(AppName),
                {lists:reverse(Apps0) ++ AppsAcc, maps:merge(Ret0, RetAcc)}
        end,
        {[], #{}},
        Apps
    ).

-type config() :: [{atom(), term()}].
-type test_case_name() :: atom().
-type group_name() :: atom().

-spec cfg(atom(), config()) -> term().
cfg(Key, Config) ->
    case lists:keyfind(Key, 1, Config) of
        {Key, V} -> V;
        _ -> undefined
    end.

%%

-spec create_client() -> dmt_client_api:t().
create_client() ->
    create_client_w_context(woody_context:new()).

-spec create_client(woody:trace_id()) -> dmt_client_api:t().
create_client(TraceID) ->
    create_client_w_context(woody_context:new(TraceID)).

create_client_w_context(WoodyCtx) ->
    dmt_client_api:new(WoodyCtx).

-spec cleanup_db() -> ok.
cleanup_db() ->
    Query = """
    DO $$
    DECLARE
        r RECORD;
    BEGIN
        -- Loop through all tables in the current schema
        FOR r IN (
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public'
            AND NOT table_name = '__migrations'
            AND NOT table_name = 'entity_type'
            ) LOOP
            -- Execute the TRUNCATE command on each table
            EXECUTE 'TRUNCATE TABLE ' || quote_ident(r.table_name) || ' RESTART IDENTITY CASCADE';
        END LOOP;
    END $$;
    """,
    {ok, _, _} = epg_pool:query(default_pool, Query),
    ok.

create_kafka_topics() ->
    TopicConfig = [
        #{
            configs => [],
            num_partitions => 1,
            assignments => [],
            replication_factor => 1,
            name => ?TEST_TOPIC
        }
    ],
    _ = brod:create_topics(?BROKERS, TopicConfig, #{timeout => 5000}).

delete_kafka_topics() ->
    _ = brod:delete_topics(?BROKERS, [?TEST_TOPIC], #{}).
