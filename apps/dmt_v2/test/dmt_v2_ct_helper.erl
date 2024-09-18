-module(dmt_v2_ct_helper).

-export([start_app/1]).
-export([start_app/2]).
-export([start_apps/1]).

-export([cfg/2]).

-export([create_client/0]).
-export([create_client/1]).

-include_lib("damsel/include/dmsl_base_thrift.hrl").
-include_lib("damsel/include/dmsl_domain_thrift.hrl").

-export_type([config/0]).
-export_type([test_case_name/0]).
-export_type([group_name/0]).

%%

-type app_name() :: atom().

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
start_app(dmt_v2 = AppName) ->
    {
        start_app(AppName, [
            {scoper_event_handler_options, #{
                event_handler_opts => #{
                    formatter_opts => #{
                        max_length => 1000
                    }
                }
            }},
            {services, #{
                repository => <<"http://dominant-v2:8022/v1/domain/repository">>,
                repository_client => <<"http://dominant-v2:8022/v1/domain/repository_client">>,
                user_op => <<"http://dominant-v2:8022/v1/domain/user_op">>
            }}
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

-spec create_client() -> dmt_v2_client_api:t().
create_client() ->
    create_client_w_context(woody_context:new()).

-spec create_client(woody:trace_id()) -> dmt_v2_client_api:t().
create_client(TraceID) ->
    create_client_w_context(woody_context:new(TraceID)).

create_client_w_context(WoodyCtx) ->
    dmt_v2_client_api:new(WoodyCtx).
