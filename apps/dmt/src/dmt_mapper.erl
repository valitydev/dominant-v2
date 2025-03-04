-module(dmt_mapper).

-include_lib("epgsql/include/epgsql.hrl").

-export([to_marshalled_maps/2]).
-export([to_marshalled_maps/3]).
-export([to_string/1]).
-export([from_string/1]).

to_marshalled_maps(Columns, Rows) ->
    to_marshalled_maps(Columns, Rows, fun marshall_object/1).

to_marshalled_maps(Columns, Rows, TransformRowFun) ->
    ColNumbers = erlang:length(Columns),
    Seq = lists:seq(1, ColNumbers),
    lists:map(
        fun(Row) ->
            Data = lists:foldl(
                fun(Pos, Acc) ->
                    #column{name = Field, type = Type} = lists:nth(Pos, Columns),
                    Acc#{Field => convert(Type, erlang:element(Pos, Row))}
                end,
                #{},
                Seq
            ),
            TransformRowFun(Data)
        end,
        Rows
    ).

%% for reference https://github.com/epgsql/epgsql#data-representation
convert(timestamp, Value) ->
    datetime_to_binary(Value);
convert(timestamptz, Value) ->
    datetime_to_binary(Value);
convert(_Type, Value) ->
    Value.

datetime_to_binary({Date, {Hour, Minute, Second}}) when is_float(Second) ->
    datetime_to_binary({Date, {Hour, Minute, trunc(Second)}});
datetime_to_binary(DateTime) ->
    UnixTime = genlib_time:daytime_to_unixtime(DateTime),
    genlib_rfc3339:format(UnixTime, second).

marshall_object(#{
    <<"id">> := ID,
    <<"entity_type">> := Type,
    <<"version">> := Version,
    <<"references_to">> := ReferencesTo,
    <<"referenced_by">> := ReferencedBy,
    <<"data">> := Data,
    <<"created_at">> := CreatedAt,
    <<"is_active">> := IsActive
}) ->
    dmt_object:just_object(
        from_string(ID),
        Type,
        Version,
        lists:map(fun from_string/1, ReferencesTo),
        lists:map(fun from_string/1, ReferencedBy),
        from_string(Data),
        CreatedAt,
        IsActive
    ).

to_string(A0) ->
    A1 = term_to_binary(A0),
    base64:encode(A1).

from_string(B0) ->
    B1 = base64:decode(B0),
    binary_to_term(B1).
