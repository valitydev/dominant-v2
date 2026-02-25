%%% @doc
%%% Logger filter для otel_logs handler: конвертирует otel_trace_id и otel_span_id
%%% из hex-формата (32/16 символов) в raw bytes (16/8 байт), как требует OTLP LogRecord.
%%% opentelemetry hex_span_ctx возвращает hex, collector ожидает bytes.
%%% @end
-module(dmt_otel_log_filter).

-export([filter/2]).
-export([format_otp_report_utf8/1]).

-spec filter(logger:log_event(), term()) -> logger:filter_return().
filter(#{meta := Meta} = LogEvent, _FilterConfig) ->
    case convert_otel_ids(Meta) of
        Meta ->
            LogEvent;
        Meta1 ->
            LogEvent#{meta => Meta1}
    end.

%% Конвертируем hex -> raw bytes только если формат hex (32/16 символов).
%% OTLP LogRecord: trace_id=16 bytes, span_id=8 bytes.
convert_otel_ids(#{otel_trace_id := TraceIdHex, otel_span_id := SpanIdHex} = Meta) ->
    case {hex_to_trace_id_bytes(TraceIdHex), hex_to_span_id_bytes(SpanIdHex)} of
        {TraceIdBytes, SpanIdBytes} when TraceIdBytes =/= undefined, SpanIdBytes =/= undefined ->
            Meta#{otel_trace_id => TraceIdBytes, otel_span_id => SpanIdBytes};
        _ ->
            %% Некорректный формат — убираем, чтобы otel_otlp_logs не отправил в OTLP
            maps:without([otel_trace_id, otel_span_id, otel_trace_flags], Meta)
    end;
convert_otel_ids(Meta) ->
    Meta.

%% logger:format_otp_report/1 возвращает chardata (часто list()),
%% из-за чего downstream JSON может сериализовать body как массив байт.
%% Явно приводим к UTF-8 binary(), чтобы body в OTel/Loki был строкой.
-spec format_otp_report_utf8(logger:report()) -> {unicode:chardata(), list()}.
format_otp_report_utf8(Report) ->
    Bin =
        try logger:format_otp_report(Report) of
            {Format, Args} ->
                unicode:characters_to_binary(io_lib:format(Format, Args))
        catch
            _:_ ->
                %% Не даём report_cb падать: fallback в печатное представление отчёта.
                unicode:characters_to_binary(io_lib:format("~tp", [Report]))
        end,
    {"~ts", [Bin]}.

hex_to_trace_id_bytes(Hex) when is_binary(Hex), byte_size(Hex) =:= 32 ->
    try
        <<(binary_to_integer(Hex, 16)):128>>
    catch
        _:_ -> undefined
    end;
hex_to_trace_id_bytes(_) ->
    undefined.

hex_to_span_id_bytes(Hex) when is_binary(Hex), byte_size(Hex) =:= 16 ->
    try
        <<(binary_to_integer(Hex, 16)):64>>
    catch
        _:_ -> undefined
    end;
hex_to_span_id_bytes(_) ->
    undefined.
