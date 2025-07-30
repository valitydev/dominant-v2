%%%-------------------------------------------------------------------
%% @doc dmt top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(dmt_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-export([get_service/1]).
-export([get_damsel_version/0]).

-define(APP, dmt).
-define(DEFAULT_DB, default_db).

start_link() ->
    supervisor:start_link({local, ?APP}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init(_) ->
    ok = dbinit(),
    ok = setup_kafka(dmt_kafka_publisher:is_kafka_enabled()),
    ok = setup_damsel_version(),
    {ok, IP} = inet:parse_address(application_get_env(?APP, ip, "::")),
    HealthCheck = enable_health_logging(application_get_env(?APP, health_check, #{})),
    EventHandlers = application_get_env(?APP, woody_event_handlers, [scoper_woody_event_handler]),
    API = woody_server:child_spec(
        ?MODULE,
        #{
            ip => IP,
            port => application_get_env(?APP, port, 8022),
            transport_opts => application_get_env(?APP, transport_opts, #{}),
            protocol_opts => application_get_env(?APP, protocol_opts, #{}),
            event_handler => EventHandlers,
            handlers => get_repository_handlers(),
            additional_routes => [
                get_prometheus_route(),
                erl_health_handle:get_route(HealthCheck)
            ]
        }
    ),
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },
    ChildSpecs = [API],

    {ok, {SupFlags, ChildSpecs}}.

dbinit() ->
    WorkDir = get_env_var("WORK_DIR"),
    _ = set_database_url(),
    MigrationsPath = WorkDir ++ "/migrations",
    Cmd = "run",
    case dmt_db_migration:process(["-d", MigrationsPath, Cmd]) of
        ok ->
            _ = logger:warning("entity_type: ~p", [
                epg_pool:query(default_pool, "SELECT * FROM entity_type;")
            ]),
            ok;
        {error, Reason} ->
            throw({migrations_error, Reason})
    end.

set_database_url() ->
    EpgDbName = application_get_env(?APP, epg_db_name, ?DEFAULT_DB),
    #{
        EpgDbName := #{
            host := PgHost,
            port := PgPort,
            username := PgUser,
            password := PgPassword,
            database := DbName
        }
    } = application_get_env(epg_connector, databases),
    %% DATABASE_URL=postgresql://postgres:postgres@db/dmtv2
    PgPortStr = erlang:integer_to_list(PgPort),
    Value =
        "postgresql://" ++ PgUser ++ ":" ++ PgPassword ++ "@" ++ PgHost ++ ":" ++ PgPortStr ++ "/" ++
            DbName,
    true = os:putenv("DATABASE_URL", Value).

%% internal functions

get_env_var(Name) ->
    case os:getenv(Name) of
        false -> throw({os_env_required, Name});
        V -> V
    end.

get_repository_handlers() ->
    DefaultTimeout = application_get_env(?APP, default_woody_handling_timeout, timer:seconds(30)),
    [
        get_handler(repository, #{
            repository => dmt_repository_handler,
            default_handling_timeout => DefaultTimeout
        }),
        get_handler(repository_client, #{
            repository => dmt_repository_client_handler,
            default_handling_timeout => DefaultTimeout
        }),
        get_handler(author, #{
            repository => dmt_author_handler,
            default_handling_timeout => DefaultTimeout
        })
    ].

-spec get_handler(repository | repository_client | author, woody:options()) ->
    woody:http_handler(woody:th_handler()).
get_handler(repository, Options) ->
    {"/v1/domain/repository", {
        get_service(repository),
        {dmt_repository_handler, Options}
    }};
get_handler(repository_client, Options) ->
    {"/v1/domain/repository_client", {
        get_service(repository_client),
        {dmt_repository_client_handler, Options}
    }};
get_handler(author, Options) ->
    {"/v1/domain/author", {
        get_service(author),
        {dmt_author_handler, Options}
    }}.

get_service(repository) ->
    {dmsl_domain_conf_v2_thrift, 'Repository'};
get_service(repository_client) ->
    {dmsl_domain_conf_v2_thrift, 'RepositoryClient'};
get_service(author) ->
    {dmsl_domain_conf_v2_thrift, 'AuthorManagement'}.

-spec enable_health_logging(erl_health:check()) -> erl_health:check().
enable_health_logging(Check) ->
    EvHandler = {erl_health_event_handler, []},
    maps:map(fun(_, {_, _, _} = V) -> #{runner => V, event_handler => EvHandler} end, Check).

-spec get_prometheus_route() -> {iodata(), module(), _Opts :: any()}.
get_prometheus_route() ->
    {"/metrics/[:registry]", prometheus_cowboy2_handler, []}.

%% @doc Setup damsel version information from multiple sources
setup_damsel_version() ->
    DamselVersionInfo = get_damsel_version(),
    logger:warning("Damsel version info: ~p", [DamselVersionInfo]),
    ok = application:set_env(?APP, damsel_version_info, DamselVersionInfo).

%% @doc Get comprehensive damsel version information
-spec get_damsel_version() ->
    #{
        app_vsn => binary() | undefined,
        git_ref => binary() | undefined,
        source => rebar_lock | app_file
    }.
get_damsel_version() ->
    % Try to get version from application
    AppVsn =
        case application:get_key(damsel, vsn) of
            {ok, Vsn} -> list_to_binary(Vsn);
            undefined -> undefined
        end,

    % Try to get git ref from rebar.lock
    GitRef = get_damsel_git_ref_from_lock(),

    #{
        app_vsn => AppVsn,
        git_ref => GitRef,
        source =>
            case GitRef of
                undefined -> app_file;
                _ -> rebar_lock
            end
    }.

%% @doc Extract damsel git reference from rebar.lock file
-spec get_damsel_git_ref_from_lock() -> binary() | undefined.
get_damsel_git_ref_from_lock() ->
    try
        WorkDir = get_env_var("WORK_DIR"),
        LockFile = WorkDir ++ "/rebar.lock",
        case file:consult(LockFile) of
            {ok, [LockData | _]} ->
                case extract_damsel_ref_from_lock_data(LockData) of
                    {ok, Ref} -> list_to_binary(Ref);
                    error -> undefined
                end;
            {error, _} ->
                undefined
        end
    catch
        _:_ -> undefined
    end.

%% @doc Extract damsel reference from parsed rebar.lock data
-spec extract_damsel_ref_from_lock_data(term()) -> {ok, string()} | error.
extract_damsel_ref_from_lock_data({_LockVersion, Deps}) when is_list(Deps) ->
    case lists:keyfind(<<"damsel">>, 1, Deps) of
        {<<"damsel">>, {git, _Url, {ref, Ref}}, _Level} ->
            {ok, Ref};
        _ ->
            error
    end;
extract_damsel_ref_from_lock_data(_) ->
    error.

application_get_env(App, Key) ->
    application_get_env(App, Key, undefined).

application_get_env(App, Key, Default) ->
    case application:get_env(App, Key) of
        {ok, Value} -> Value;
        undefined -> Default
    end.

setup_kafka(false) ->
    ok;
setup_kafka(_) ->
    ClientName = dmt_kafka_client,
    Clients = application_get_env(brod, clients, []),
    Client = proplists:get_value(ClientName, Clients),
    Endpoints = proplists:get_value(endpoints, Client),
    ClientConfig = proplists:delete(endpoints, Client),

    _ = logger:info("Starting Kafka client ~p with endpoints ~p and config ~p", [
        ClientName, Endpoints, ClientConfig
    ]),
    case brod:start_client(Endpoints, ClientName, ClientConfig) of
        ok ->
            ok;
        {error, already_present} ->
            _ = logger:info("Kafka client ~p already present", [ClientName]),
            ok;
        {error, Reason} ->
            logger:error("Failed to start Kafka client ~p: ~p", [ClientName, Reason]),
            throw({kafka_client_start_error, Reason})
    end.
