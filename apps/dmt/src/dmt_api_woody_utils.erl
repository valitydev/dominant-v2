-module(dmt_api_woody_utils).

-export([get_woody_client/1]).
-export([ensure_woody_deadline_set/2]).

%% API

-type service_opts() :: #{
    url := binary() | list(),
    transport_opts => term(),
    resolver_opts => term()
}.

%% Options passed to every thrift handler via woody's `handlers` config.
-type handler_options() :: #{default_handling_timeout := timeout(), atom() => term()}.

-export_type([service_opts/0, handler_options/0]).

-spec get_woody_client(atom()) -> woody_client:options().
get_woody_client(Service) ->
    Services = get_services(genlib_app:env(dmt_api, services, #{})),
    make_woody_client(maps:get(Service, Services)).

-spec get_services(term()) -> #{atom() => service_opts()}.
get_services(Map) when is_map(Map) ->
    maps:fold(fun fold_service/3, #{}, Map);
get_services(_) ->
    #{}.

-spec fold_service(term(), term(), #{atom() => service_opts()}) ->
    #{atom() => service_opts()}.
fold_service(K, #{url := Url} = V, Acc) when is_atom(K), is_binary(Url) ->
    Acc#{K => build_service_opts(V, Url)};
fold_service(K, #{url := Url} = V, Acc) when is_atom(K), is_list(Url) ->
    Acc#{K => build_service_opts(V, Url)};
fold_service(_, _, Acc) ->
    Acc.

-spec build_service_opts(map(), binary() | list()) -> service_opts().
build_service_opts(V, Url) ->
    Base = #{url => Url},
    Base1 =
        case maps:find(transport_opts, V) of
            {ok, T} -> Base#{transport_opts => T};
            error -> Base
        end,
    case maps:find(resolver_opts, V) of
        {ok, R} -> Base1#{resolver_opts => R};
        error -> Base1
    end.

-spec make_woody_client(service_opts()) -> woody_client:options().
make_woody_client(#{url := Url} = Service) ->
    lists:foldl(
        fun(Opt, Acc) ->
            case maps:get(Opt, Service, undefined) of
                undefined -> Acc;
                Value -> Acc#{Opt => Value}
            end
        end,
        #{
            url => Url,
            event_handler => get_woody_event_handlers()
        },
        [
            transport_opts,
            resolver_opts
        ]
    ).

-spec get_woody_event_handlers() -> woody:ev_handlers().
get_woody_event_handlers() ->
    Default = [scoper_woody_event_handler],
    case genlib_app:env(dmt_api, woody_event_handlers, Default) of
        Handler when is_atom(Handler) -> Handler;
        {Mod, Opts} when is_atom(Mod) -> {Mod, Opts};
        [_ | _] = List -> [ensure_ev_handler(H) || H <- List];
        [] -> Default;
        _ -> Default
    end.

-spec ensure_ev_handler(term()) -> woody:ev_handler() | no_return().
ensure_ev_handler(Mod) when is_atom(Mod) -> Mod;
ensure_ev_handler({Mod, Opts}) when is_atom(Mod) -> {Mod, Opts};
ensure_ev_handler(Other) -> erlang:error({bad_ev_handler, Other}).

-spec ensure_woody_deadline_set(woody_context:ctx(), woody_deadline:deadline()) -> woody_context:ctx().
ensure_woody_deadline_set(WoodyContext, Default) ->
    case woody_context:get_deadline(WoodyContext) of
        undefined ->
            woody_context:set_deadline(Default, WoodyContext);
        _Other ->
            WoodyContext
    end.
