-module(dmt_db_migration).

%% API exports
-export([run/1]).

-define(APP, dmt).
-define(DEFAULT_DB, default_db).
-define(REALM, "dmt").

%%====================================================================
%% API functions
%%====================================================================

%% Applies pending migrations from MigrationsDir using epg_migrator.
%% Beforehand, history recorded by the legacy psql-migration tool (the
%% `__migrations` table) is carried over into epg_migrator's
%% `schema_migrations` table, so migrations applied before the
%% switchover are not rerun.
-spec run(string()) -> ok | {error, term()}.
run(MigrationsDir) ->
    DbOpts = db_opts(),
    case backfill_legacy_history(DbOpts) of
        ok ->
            perform(DbOpts, MigrationsDir);
        {error, Reason} ->
            {error, {legacy_history_backfill, Reason}}
    end.

%%====================================================================
%% Internal functions
%%====================================================================

perform(DbOpts, MigrationsDir) ->
    case epg_migrator:perform(?REALM, DbOpts, [], MigrationsDir) of
        {ok, Executed} ->
            _ = logger:info("Applied migrations: ~p", [Executed]),
            ok;
        {error, _} = Error ->
            Error
    end.

db_opts() ->
    EpgDbName =
        case application:get_env(?APP, epg_db_name) of
            {ok, Name} -> Name;
            undefined -> ?DEFAULT_DB
        end,
    {ok, Databases} = application:get_env(epg_connector, databases),
    maps:get(EpgDbName, Databases).

backfill_legacy_history(DbOpts) ->
    case epgsql:connect(maps:put(timeout, 10000, DbOpts)) of
        {ok, Conn} ->
            try
                Fun = fun(C) -> do_backfill(C, DbOpts) end,
                case epgsql:with_transaction(Conn, Fun, [{reraise, false}]) of
                    ok -> ok;
                    {rollback, Reason} -> {error, Reason}
                end
            after
                ok = epgsql:close(Conn)
            end;
        {error, Reason} ->
            {error, Reason}
    end.

do_backfill(C, #{database := Database}) ->
    ok = take_migration_lock(C, Database),
    case legacy_table_exists(C) of
        true -> copy_legacy_history(C);
        false -> ok
    end.

%% Same lock key as epg_migrator uses, so the backfill serializes with
%% migrator runs of other replicas starting concurrently.
take_migration_lock(C, Database) ->
    LockKey = erlang:phash2(Database),
    {ok, _, _} = epgsql:equery(C, "SELECT pg_advisory_xact_lock($1)", [LockKey]),
    ok.

legacy_table_exists(C) ->
    Query = """
    SELECT EXISTS (
        SELECT FROM pg_catalog.pg_tables
        WHERE schemaname = current_schema() AND tablename = '__migrations'
    )
    """,
    {ok, _, [{Exists}]} = epgsql:squery(C, Query),
    Exists =:= <<"t">>.

copy_legacy_history(C) ->
    ok = ensure_history_table(C),
    Query = """
    INSERT INTO schema_migrations (realm, migration_file_name, executed_at)
    SELECT $1, regexp_replace(id, '^.*/', '') || '.sql', COALESCE(datetime, NOW())
    FROM __migrations
    ON CONFLICT DO NOTHING
    """,
    Result = epgsql:equery(C, Query, [list_to_binary(?REALM)]),
    case Result of
        {ok, Count} ->
            _ = logger:info("Carried over ~p legacy migration records", [Count]),
            ok;
        {error, Reason} ->
            erlang:error({legacy_history_copy, Reason})
    end.

%% Same definition as epg_migrator_storage uses; created here ahead of
%% time so legacy history can be inserted before the first perform.
ensure_history_table(C) ->
    Query = """
    CREATE TABLE IF NOT EXISTS schema_migrations (
        realm VARCHAR(255) NOT NULL,
        migration_file_name VARCHAR(255) NOT NULL,
        executed_at TIMESTAMP NOT NULL DEFAULT NOW(),
        PRIMARY KEY (realm, migration_file_name)
    )
    """,
    Result = epgsql:squery(C, Query),
    case Result of
        {error, Reason} -> erlang:error({create_history_table, Reason});
        _Ok -> ok
    end.
