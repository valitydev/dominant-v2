-module(dmt_db_migration).

%% API exports
-export([process/1]).

%%====================================================================
%% API functions
%%====================================================================

-define(OPTS, [
    {dir, $d, "dir", {string, "migrations"}, "Migration folder"},
    {env, $e, "env", {string, ".env"}, "Environment file to search for DATABASE_URL"}
]).

-spec process(list()) -> ok | {error, any()}.
process(Args) ->
    handle_command(getopt:parse(?OPTS, Args)).

handle_command({error, Error}) ->
    logger:error("~p", [Error]),
    {error, Error};
handle_command({ok, {Args, ["new", Name]}}) ->
    Dir = target_dir(Args),
    Timestamp = erlang:system_time(seconds),
    MigrationName = [integer_to_list(Timestamp), "-", Name, ".sql"],
    Filename = filename:join(Dir, MigrationName),
    C = [
        "-- ",
        Filename,
        "\n",
        "-- :up\n",
        "-- Up migration\n\n",
        "-- :down\n",
        "-- Down migration\n"
    ],
    Result =
        case file:write_file(Filename, list_to_binary(C), [exclusive]) of
            ok -> {ok, "Created migration: ~s~n", [Filename]};
            {error, Reason} -> {error, "Migration can not be written to file ~s: ~s~n", [Filename, Reason]}
        end,
    handle_command_result(Result);
handle_command({ok, {Args, ["run"]}}) ->
    Available = available_migrations(Args),
    Result = with_connection(
        Args,
        fun(Conn) ->
            Applied = applied_migrations(Conn),
            ToApply = lists:filter(fun({Mig, _}) -> not lists:member(Mig, Applied) end, Available),
            Results = apply_migrations(up, ToApply, Conn),

            report_migrations(up, Results)
        end
    ),
    handle_command_result(Result);
handle_command({ok, {Args, ["revert"]}}) ->
    Available = available_migrations(Args),
    Result = with_connection(
        Args,
        fun(Conn) ->
            case applied_migrations(Conn) of
                [] ->
                    {error, "No applied migrations to revert"};
                Applied ->
                    LastApplied = lists:last(Applied),
                    case lists:keyfind(LastApplied, 1, Available) of
                        false ->
                            {error, "Migration ~p can not be found~n", [LastApplied]};
                        Migration ->
                            Results = apply_migrations(down, [Migration], Conn),
                            report_migrations(down, Results)
                    end
            end
        end
    ),
    handle_command_result(Result);
handle_command({ok, {Args, ["reset"]}}) ->
    {ok, Opts} = connection_opts(Args),
    case maps:take(database, Opts) of
        error ->
            handle_command_result({error, "No database to reset~n"});
        {Database, Opts1} ->
            case
                with_connection(
                    Opts1#{database => "postgres"},
                    fun(Conn) ->
                        if_ok(epgsql:equery(Conn, "drop database if exists " ++ Database))
                    end
                )
            of
                ok ->
                    handle_command({ok, {Args, ["setup"]}});
                Other ->
                    handle_command_result(Other)
            end
    end;
handle_command({ok, {Args, ["setup"]}}) ->
    {ok, Opts} = connection_opts(Args),
    case maps:take(database, Opts) of
        error ->
            handle_command_result({error, "No database to set up~n"});
        {Database, Opts1} ->
            case
                with_connection(
                    Opts1#{database => "postgres"},
                    fun(Conn) ->
                        case
                            epgsql:squery(
                                Conn,
                                "select 1 from pg_database where datname = '" ++ Database ++ "'"
                            )
                        of
                            {ok, _, []} ->
                                if_ok(epgsql:squery(Conn, "create database " ++ Database));
                            Other ->
                                if_ok(Other)
                        end
                    end
                )
            of
                ok ->
                    handle_command({ok, {Args, ["run"]}});
                Other ->
                    handle_command_result(Other)
            end
    end;
handle_command({ok, {_, _}}) ->
    ok.

%% Utils

-type command_result() ::
    ok
    | {ok, io:format(), [term()]}
    | {error, string()}
    | {error, io:format(), [term()]}.

-spec handle_command_result(command_result()) -> ok | {error, term()}.
handle_command_result(ok) ->
    logger:info("All sql migrations completed"),
    ok;
handle_command_result({ok, Fmt, Args}) ->
    logger:info(Fmt, Args),
    ok;
handle_command_result({error, Str}) ->
    handle_command_result({error, Str, []});
handle_command_result({error, Fmt, Args}) ->
    logger:error(Fmt, Args),
    {error, Fmt}.

-spec with_connection(list() | map(), fun((pid()) -> command_result())) ->
    command_result().
with_connection(Args, Fun) ->
    case open_connection(Args) of
        {ok, Conn} ->
            Fun(Conn);
        {error, Error} ->
            {error, io_lib:format("Failed to connect to database: ~p~n", [Args]), [Error]}
    end.

connection_opts(Args) ->
    connection_opts(Args, {env, "DATABASE_URL"}).

connection_opts(Args, {env, URLName}) ->
    maybe_load_dot_env(dot_env(Args)),
    case os:getenv(URLName) of
        false -> {error, "Missing DATABASE_URL.~n"};
        DatabaseUrl -> connection_opts(Args, {url, DatabaseUrl})
    end;
connection_opts(_Args, {url, DatabaseUrl}) ->
    case uri_string:parse(string:trim(DatabaseUrl)) of
        {error, Error, Term} ->
            {error, {Error, Term}};
        Map = #{userinfo := UserPass, host := Host, path := Path} ->
            {User, Pass} =
                case string:split(UserPass, ":") of
                    [[]] -> {"postgres", ""};
                    [U] -> {U, ""};
                    [[], []] -> {"postgres", ""};
                    [U, P] -> {U, P}
                end,

            ConnectionOpts = #{
                port => maps:get(port, Map, 5432),
                username => User,
                password => Pass,
                host => Host,
                database => string:slice(Path, 1)
            },

            case maps:get(query, Map, []) of
                [] ->
                    {ok, ConnectionOpts};
                "?" ++ QueryString ->
                    case uri_string:dissect_query(QueryString) of
                        [] ->
                            {ok, ConnectionOpts};
                        QueryList ->
                            case proplists:get_value("ssl", QueryList) of
                                undefined -> {ok, ConnectionOpts};
                                [] -> {ok, maps:put(ssl, true, ConnectionOpts)};
                                Value -> {ok, maps:put(ssl, list_to_atom(Value), ConnectionOpts)}
                            end
                    end
            end
    end.

-spec open_connection(list() | map()) -> {ok, epgsql:connection()} | {error, term()}.
open_connection(Args) when is_list(Args) ->
    {ok, Opts} = connection_opts(Args),
    open_connection(Opts);
open_connection(Opts) ->
    epgsql:connect(Opts).

target_dir(Args) ->
    case lists:keyfind(dir, 1, Args) of
        false ->
            ".";
        {dir, Dir} ->
            _ = filelib:ensure_dir(Dir),
            Dir
    end.

maybe_load_dot_env(DotEnv) ->
    case filelib:is_file(DotEnv) of
        true -> envloader:load(DotEnv);
        % NB: Ignore the missing file.
        false -> ok
    end.

dot_env(Args) ->
    case lists:keyfind(env, 1, Args) of
        false -> ".env";
        {env, DotEnv} -> DotEnv
    end.

-spec report_migrations(up | down, [{Version :: string(), ok | {error, term()}}]) -> ok.
report_migrations(_, L) when length(L) == 0 ->
    logger:warning("No migrations were run");
report_migrations(up, Results) ->
    [logger:info("Applied ~s: ~p", [V, R]) || {V, R} <- Results],
    ok;
report_migrations(down, Results) ->
    [logger:info("Reverted ~s: ~p", [V, R]) || {V, R} <- Results],
    ok.

-define(DRIVER, epgsql).

record_migration(up, Conn, V) ->
    epgsql:equery(Conn, "INSERT INTO __migrations (id) VALUES ($1)", [V]);
record_migration(down, Conn, V) ->
    epgsql:equery(Conn, "DELETE FROM __migrations WHERE id = $1", [V]).

apply_migrations(Type, Migrations, Conn) ->
    Results = lists:foldl(
        fun
            (_, [{_, {error, _}} | _] = Acc) ->
                Acc;
            ({Version, _} = Migration, Acc) ->
                case apply_migration(Type, Migration, Conn) of
                    ok -> [{Version, ok} | Acc];
                    {error, Error} -> [{Version, {error, Error}}]
                end
        end,
        [],
        Migrations
    ),
    lists:reverse(Results).

apply_migration(Type, {Version, Migration}, Conn) ->
    case eql:get_query(Type, Migration) of
        {ok, Query} ->
            case if_ok(epgsql:squery(Conn, Query)) of
                ok ->
                    _ = record_migration(Type, Conn, Version),
                    ok;
                {error, Error} ->
                    {error, Error}
            end;
        undefined ->
            _ = record_migration(Type, Conn, Version),
            ok
    end.

if_ok(Rs) when is_list(Rs) ->
    Result = lists:map(fun(R) -> if_ok(R) end, Rs),
    case lists:keyfind(error, 1, Result) of
        false -> ok;
        Error -> Error
    end;
if_ok({ok, _}) ->
    ok;
if_ok({ok, _, _}) ->
    ok;
if_ok({ok, _, _, _}) ->
    ok;
if_ok({error, {error, error, _, _, Descr, _}}) ->
    {error, binary_to_list(Descr)};
if_ok(Error) ->
    {error, Error}.

available_migrations(Args) ->
    Dir = target_dir(Args),
    Files = filelib:wildcard(filename:join(Dir, "*.sql")),
    lists:map(
        fun(Filename) ->
            {ok, Migs} = eql:compile(Filename),
            {filename:rootname(Filename), Migs}
        end,
        lists:usort(Files)
    ).

applied_migrations(Args) when is_list(Args) ->
    with_connection(
        Args,
        fun(Conn) ->
            applied_migrations(Conn)
        end
    );
applied_migrations(Conn) when is_pid(Conn) ->
    case epgsql:squery(Conn, "SELECT id FROM __migrations ORDER by id ASC") of
        {ok, _, Migs} ->
            [binary_to_list(Mig) || {Mig} <- Migs];
        {error, {error, error, <<"42P01">>, _, _, _}} ->
            %% init migrations and restart
            {ok, _, _} = epgsql:squery(
                Conn,
                "CREATE TABLE __migrations ("
                "id VARCHAR(255) PRIMARY KEY,"
                "datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            ),
            applied_migrations(Conn)
    end.
