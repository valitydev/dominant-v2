-module(dmt_thrift).

-export([encode/3]).
-export([decode/3]).

-export([get_domain_object_thrift_type/1]).
-export([get_domain_object_reference_thrift_type/1]).
-export([get_commit_ops_type/0]).

-export_type([thrift_type/0]).
-export_type([function_schema/0]).
-export_type([struct_type/0]).
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

-spec get_domain_object_thrift_type(atom()) -> struct_type().
get_domain_object_thrift_type(DomainObjectType) ->
    {struct, union, DomainObjectTypes} = dmsl_domain_thrift:struct_info('DomainObject'),
    get_thrift_type(DomainObjectType, DomainObjectTypes).

get_thrift_type(Type, []) ->
    erlang:throw({notfound, Type});
get_thrift_type(Type, [{_, _, ThriftType, Type, _} | _]) ->
    ThriftType;
get_thrift_type(Type, [_ | ThriftDefinitions]) ->
    get_thrift_type(Type, ThriftDefinitions).

-spec get_domain_object_reference_thrift_type(atom()) -> struct_type().
get_domain_object_reference_thrift_type(Type) ->
    {struct, union, DomainObjectReferenceTypes} = dmsl_domain_thrift:struct_info('Reference'),
    get_thrift_type(Type, DomainObjectReferenceTypes).

-spec get_commit_ops_type() -> {list, thrift_type()}.
get_commit_ops_type() ->
    {list, {struct, union, {dmsl_domain_conf_thrift, 'Operation'}}}.
