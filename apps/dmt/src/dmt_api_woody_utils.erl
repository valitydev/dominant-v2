-module(dmt_api_woody_utils).

-export([get_woody_client/1]).
-export([ensure_woody_deadline_set/2]).

%% API

-spec get_woody_client(atom()) -> woody_client:options().
get_woody_client(Service) ->
    Services = genlib_app:env(dmt_api, services, #{}),
    make_woody_client(maps:get(Service, Services)).

-spec make_woody_client(#{atom() => _}) -> woody_client:options().
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
    genlib_app:env(dmt_api, woody_event_handlers, [scoper_woody_event_handler]).

-spec ensure_woody_deadline_set(woody_context:ctx(), woody_deadline:deadline()) -> woody_context:ctx().
ensure_woody_deadline_set(WoodyContext, Default) ->
    case woody_context:get_deadline(WoodyContext) of
        undefined ->
            woody_context:set_deadline(Default, WoodyContext);
        _Other ->
            WoodyContext
    end.
