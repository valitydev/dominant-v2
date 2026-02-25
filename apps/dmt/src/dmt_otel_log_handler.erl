-module(dmt_otel_log_handler).

-export([log/2]).
-export([adding_handler/1]).
-export([removing_handler/1]).
-export([changing_config/3]).
-export([filter_config/1]).

-spec log(logger:log_event(), map()) -> ok.
log(LogEvent, Config) ->
    otel_log_handler:log(LogEvent, Config).

-spec adding_handler(map()) -> {ok, map()} | {error, term()}.
adding_handler(Config) ->
    otel_log_handler:adding_handler(merge_module_config(Config)).

-spec removing_handler(map()) -> ok.
removing_handler(Config) ->
    otel_log_handler:removing_handler(Config).

-spec changing_config(set | update, map(), map()) -> {ok, map()} | {error, term()}.
changing_config(SetOrUpdate, OldConfig, NewConfig) ->
    otel_log_handler:changing_config(
        SetOrUpdate,
        merge_module_config(OldConfig),
        merge_module_config(NewConfig)
    ).

-spec filter_config(map()) -> map().
filter_config(Config) ->
    otel_log_handler:filter_config(Config).

%% Переносим ключи из вложенного config в корневой map для otel_log_handler.
%% Только ключи, которые ожидает otel_log_handler — чтобы случайно не перезаписать
%% верхнеуровневые настройки logger (level, filters, filter_default и т.д.).
-define(OTEL_LOG_HANDLER_KEYS, [
    exporter,
    report_cb,
    max_queue_size,
    scheduled_delay_ms,
    exporting_timeout_ms
]).

merge_module_config(#{config := ModuleConfig} = Config) when is_map(ModuleConfig) ->
    OtelConfig = maps:with(?OTEL_LOG_HANDLER_KEYS, ModuleConfig),
    maps:merge(Config, OtelConfig);
merge_module_config(Config) ->
    Config.
