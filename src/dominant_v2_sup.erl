%%%-------------------------------------------------------------------
%% @doc dominant_v2 top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(dominant_v2_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    ok = dbinit(),
    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [],
    {ok, {SupFlags, ChildSpecs}}.

get_env_var(Name) ->
  case os:getenv(Name) of
    false -> throw({os_env_required, Name});
    V -> V
  end.

dbinit() ->
  WorkDir = get_env_var("WORK_DIR"),
  _ = set_database_url(),
  MigrationsPath = WorkDir ++ "/migrations",
  Cmd = "run",
  case dominant_v2_db_migration:process(["-d", MigrationsPath, Cmd]) of
    ok -> ok;
    {error, Reason} -> throw({migrations_error, Reason})
  end.

set_database_url() ->
  {ok, #{
    host := PgHost,
    port := PgPort,
    username := PgUser,
    password := PgPassword,
    database := DbName
  }} = application:get_env(dominant_v2, epsql_connection),
  %% DATABASE_URL=postgresql://postgres:postgres@db/dmtv2
  PgPortStr = erlang:integer_to_list(PgPort),
  Value =
    "postgresql://" ++ PgUser ++ ":" ++ PgPassword ++ "@" ++ PgHost ++ ":" ++ PgPortStr ++ "/" ++ DbName,
  true = os:putenv("DATABASE_URL", Value).

%% internal functions
