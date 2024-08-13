%%%-------------------------------------------------------------------
%% @doc dmt_v2 top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(dmt_v2_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).
-define(APP, dmt_v2).
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
    {ok, IP} = inet:parse_address(genlib_app:env(?APP, ip, "::")),
    HealthCheck = enable_health_logging(genlib_app:env(?APP, health_check, #{})),
    EventHandlers = genlib_app:env(?APP, woody_event_handlers, [scoper_woody_event_handler]),
    API = woody_server:child_spec(
        ?MODULE,
        #{
            ip => IP,
            port => genlib_app:env(?APP, port, 8022),
            transport_opts => genlib_app:env(?APP, transport_opts, #{}),
            protocol_opts => genlib_app:env(?APP, protocol_opts, #{}),
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
    case dmt_v2_db_migration:process(["-d", MigrationsPath, Cmd]) of
        ok -> ok;
        {error, Reason} -> throw({migrations_error, Reason})
    end.

set_database_url() ->
    {ok, #{
        ?DEFAULT_DB := #{
            host := PgHost,
            port := PgPort,
            username := PgUser,
            password := PgPassword,
            database := DbName
        }
    }} = application:get_env(epg_connector, databases),
    %% DATABASE_URL=postgresql://postgres:postgres@db/dmtv2
    PgPortStr = erlang:integer_to_list(PgPort),
    Value =
        "postgresql://" ++ PgUser ++ ":" ++ PgPassword ++ "@" ++ PgHost ++ ":" ++ PgPortStr ++ "/" ++ DbName,
    true = os:putenv("DATABASE_URL", Value).

%% internal functions

get_env_var(Name) ->
    case os:getenv(Name) of
        false -> throw({os_env_required, Name});
        V -> V
    end.

get_repository_handlers() ->
    Repository = genlib_app:env(?APP, repository, dmt_v2_repository_handler),
    DefaultTimeout = genlib_app:env(?APP, default_woody_handling_timeout, timer:seconds(30)),
    [
        get_handler(repository, #{
            repository => Repository,
            default_handling_timeout => DefaultTimeout
        }),
        get_handler(repository_client, #{
            repository => Repository,
            default_handling_timeout => DefaultTimeout
        })
    ].

-spec get_handler(repository | repository_client | state_processor, woody:options()) ->
    woody:http_handler(woody:th_handler()).
get_handler(repository, Options) ->
    {"/v1/domain/repository", {
        {dmsl_domain_conf_thrift, 'Repository'},
        {dmt_api_repository_handler, Options}
    }};
get_handler(repository_client, Options) ->
    {"/v1/domain/repository_client", {
        {dmsl_domain_conf_thrift, 'RepositoryClient'},
        {dmt_api_repository_client_handler, Options}
    }}.

-spec enable_health_logging(erl_health:check()) -> erl_health:check().
enable_health_logging(Check) ->
    EvHandler = {erl_health_event_handler, []},
    maps:map(fun(_, V = {_, _, _}) -> #{runner => V, event_handler => EvHandler} end, Check).

-spec get_prometheus_route() -> {iodata(), module(), _Opts :: any()}.
get_prometheus_route() ->
    {"/metrics/[:registry]", prometheus_cowboy2_handler, []}.
