-module(dmt_thrift).

-export([encode/3]).
-export([decode/3]).

-export_type([thrift_type/0]).
-export_type([function_schema/0]).
-export_type([thrift_value/0]).

-type thrift_value() :: term().

-type thrift_type() ::
    base_type()
    | collection_type()
    | enum_type()
    | struct_type().

-type base_type() ::
    bool
    | double
    | i8
    | i16
    | i32
    | i64
    | string.

-type collection_type() ::
    {list, thrift_type()}
    | {set, thrift_type()}
    | {map, thrift_type(), thrift_type()}.

-type enum_type() ::
    {enum, type_ref()}.

-type struct_type() ::
    {struct, struct_flavor(), type_ref()}.

-type struct_flavor() :: struct | union | exception.

-type type_ref() :: {module(), Name :: atom()}.

-type function_schema() :: tuple().

-spec encode(binary, thrift_type(), thrift_value()) -> binary().
encode(binary, Type, Value) ->
    Codec0 = thrift_strict_binary_codec:new(),
    {ok, Codec} = thrift_strict_binary_codec:write(Codec0, Type, Value),
    thrift_strict_binary_codec:close(Codec).

-spec decode(binary, thrift_type(), binary()) -> thrift_value().
decode(binary, Type, Data) ->
    Codec = thrift_strict_binary_codec:new(Data),
    {ok, Value, Leftovers} = thrift_strict_binary_codec:read(Codec, Type),
    <<>> = thrift_strict_binary_codec:close(Leftovers),
    Value.
