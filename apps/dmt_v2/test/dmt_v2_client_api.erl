-module(dmt_v2_client_api).

-export([new/1]).
-export([call/4]).

-export_type([t/0]).

%%

-type t() :: woody_context:ctx().

-spec new(woody_context:ctx()) -> t().
new(Context) ->
    Context.

-spec call(Name :: atom(), woody:func(), [any()], t()) -> {ok, _Response} | {exception, _} | {error, _}.
call(ServiceName, Function, Args, Context) ->
    Service = dmt_v2_sup:get_service(ServiceName),
    Request = {Service, Function, list_to_tuple(Args)},
    Opts = get_opts(ServiceName),
    try
        woody_client:call(Request, Opts, Context)
    catch
        error:Error:ST ->
            {error, {Error, ST}}
    end.

get_opts(ServiceName) ->
    EventHandlerOpts = genlib_app:env(dmt_v2, scoper_event_handler_options, #{}),
    Opts0 = #{
        event_handler => {scoper_woody_event_handler, EventHandlerOpts}
    },
    case maps:get(ServiceName, genlib_app:env(dmt_v2, services), undefined) of
        #{} = Opts ->
            maps:merge(Opts, Opts0);
        _ ->
            Opts0
    end.
