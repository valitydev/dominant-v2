-module(dmt_mapper).

-include_lib("epgsql/include/epgsql.hrl").

-export([to_marshalled_maps/2]).
-export([to_marshalled_maps/3]).
-export([marshall_object/1]).
-export([datetime_to_binary/1]).
-export([to_string/1]).
-export([from_string/1]).
-export([extract_searchable_text_from_term/1]).

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
    <<"data">> := Data,
    <<"created_at">> := CreatedAt,
    <<"is_active">> := IsActive
}) ->
    dmt_object:just_object(
        from_string(ID),
        Type,
        Version,
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

%% Process terms recursively and build a list of text fragments
extract_text(Term, Acc) when is_binary(Term) ->
    % Convert binary to string safely
    case unicode:characters_to_list(Term) of
        {error, _, _} -> Acc;
        {incomplete, _, _} -> Acc;
        String -> [String | Acc]
    end;
extract_text(Term, Acc) when is_atom(Term) ->
    % Convert atoms to strings (except special atoms)
    case Term of
        undefined -> Acc;
        null -> Acc;
        _ -> [atom_to_list(Term) | Acc]
    end;
extract_text(Term, Acc) when is_integer(Term) ->
    % Convert integers to strings
    [integer_to_list(Term) | Acc];
extract_text(Term, Acc) when is_float(Term) ->
    % Convert floats to strings with reasonable precision
    [io_lib:format("~.2f", [Term]) | Acc];
extract_text(Term, Acc) when is_list(Term) ->
    % Check if it's a string (list of integers representing characters)
    case io_lib:printable_list(Term) of
        true ->
            % It's a printable string
            [Term | Acc];
        false ->
            % It's a list of other terms, process each one
            lists:foldl(fun(Item, AccIn) -> extract_text(Item, AccIn) end, Acc, Term)
    end;
extract_text(Term, Acc) when is_tuple(Term) ->
    % Convert tuple to list and process
    TupleAsList = tuple_to_list(Term),
    extract_text(TupleAsList, Acc);
extract_text(Term, Acc) when is_map(Term) ->
    % Process both keys and values in the map
    maps:fold(
        fun(K, V, AccIn) ->
            % Process key
            AccWithKey = extract_text(K, AccIn),
            % Process value
            extract_text(V, AccWithKey)
        end,
        Acc,
        Term
    );
extract_text(_, Acc) ->
    % Any other term type is ignored
    Acc.

%% Convert the accumulated list to a single string
join_text_list(TextList) ->
    string:join(lists:reverse(TextList), " ").

%% @doc Extract searchable text from an Erlang term and return as string
extract_searchable_text_from_term(Term) ->
    TextList = extract_text(Term, []),
    join_text_list(TextList).
