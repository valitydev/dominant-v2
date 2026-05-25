-module(dmt_client_api).

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
    %% Cast: `dmt_sup:get_service/1` accepts a specific atom union
    %% (`repository | repository_client | author`); test callers pass a
    %% generic `atom()` and the runtime contract holds.
    Service = dmt_sup:get_service(eqwalizer:dynamic_cast(ServiceName)),
    Request = {Service, Function, list_to_tuple(Args)},
    Opts = get_opts(ServiceName),
    try
        woody_client:call(Request, Opts, Context)
    catch
        error:Error:ST ->
            {error, {Error, ST}}
    end.

get_opts(ServiceName) ->
    EventHandlerOpts = genlib_app:env(dmt, scoper_event_handler_options, #{}),
    Opts0 = #{
        event_handler => {scoper_woody_event_handler, EventHandlerOpts}
    },
    %% Cast: `genlib_app:env/3` returns `term()` because sys.config is opaque
    %% to the type system; `maps:get/3` expects a map argument so we assert
    %% the runtime shape here.
    case maps:get(ServiceName, eqwalizer:dynamic_cast(genlib_app:env(dmt, services, #{})), undefined) of
        #{} = Opts ->
            maps:merge(Opts, Opts0);
        _ ->
            Opts0
    end.
