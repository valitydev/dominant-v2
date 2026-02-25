%%%-------------------------------------------------------------------
%% @doc dmt public API
%% @end
%%%-------------------------------------------------------------------

-module(dmt_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    case ensure_otel_log_handler() of
        ok ->
            dmt_sup:start_link();
        {error, Reason} ->
            logger:error("Failed to add otel_logs handler: ~p", [Reason]),
            {error, Reason}
    end.

stop(_State) ->
    ok = flush_otel_logs(),
    ok.

%% internal functions

ensure_otel_log_handler() ->
    case logger:get_handler_config(otel_logs) of
        {ok, _} ->
            ok;
        _ ->
            MaxQueue = application:get_env(dmt, otel_log_max_queue_size, 2048),
            DelayMs = application:get_env(dmt, otel_log_scheduled_delay_ms, 1000),
            TimeoutMs = application:get_env(dmt, otel_log_exporting_timeout_ms, 300000),
            LogLevel = application:get_env(dmt, otel_log_level, info),
            HandlerConfig = #{
                report_cb => fun dmt_otel_log_filter:format_otp_report_utf8/1,
                exporter =>
                    {otel_exporter_logs_otlp, #{
                        protocol => http_protobuf,
                        ssl_options => []
                    }},
                max_queue_size => MaxQueue,
                scheduled_delay_ms => DelayMs,
                exporting_timeout_ms => TimeoutMs
            },
            LoggerHandlerConfig = #{
                level => LogLevel,
                filter_default => log,
                filters => [{dmt_otel_trace_id_bytes, {fun dmt_otel_log_filter:filter/2, undefined}}],
                config => HandlerConfig
            },
            case logger:add_handler(otel_logs, dmt_otel_log_handler, LoggerHandlerConfig) of
                ok ->
                    ok;
                {error, {already_exist, _}} ->
                    ok;
                {error, Reason} ->
                    {error, {otel_log_handler_failed, Reason}}
            end
    end.

%% @doc Ждём отправки буферизованных логов перед остановкой.
%% otel_log_handler батчит логи и отправляет по таймеру (scheduled_delay_ms).
%% Явного API для flush у otel_log_handler нет, поэтому ждём один полный цикл
%% батчинга + запас на сетевую отправку (export overhead).
-define(FLUSH_EXPORT_OVERHEAD_MS, 700).
-define(FLUSH_MAX_WAIT_MS, 5000).

flush_otel_logs() ->
    case logger:get_handler_config(otel_logs) of
        {ok, HandlerCfg} ->
            Config = maps:get(config, HandlerCfg, #{}),
            DelayMs = maps:get(
                scheduled_delay_ms,
                Config,
                maps:get(scheduled_delay_ms, HandlerCfg, 1000)
            ),
            _ = logger:info("otel_log_handler_flush"),
            timer:sleep(erlang:min(?FLUSH_MAX_WAIT_MS, DelayMs + ?FLUSH_EXPORT_OVERHEAD_MS)),
            ok;
        _ ->
            ok
    end.
