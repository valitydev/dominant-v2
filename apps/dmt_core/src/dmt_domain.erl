-module(dmt_domain).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

-compile({parse_transform, dmt_domain_pt}).

%%

-export([references/1]).
-export([get_data/1]).
-export([maybe_get_domain_object_data_field/2]).

-define(DOMAIN, dmsl_domain_thrift).

-export_type([operation_error/0]).
-export_type([domain_object/0]).
-export_type([object_ref/0]).

%%

-type object_ref() :: dmsl_domain_thrift:'Reference'().
-type domain_object() :: dmsl_domain_thrift:'DomainObject'().

-type nonexistent_object() :: {object_ref(), [object_ref()]}.
-type operation_conflict() ::
    {object_already_exists, object_ref()}
    | {object_not_found, object_ref()}
    | {object_reference_mismatch, object_ref()}.

-type operation_invalid() ::
    {objects_not_exist, [nonexistent_object()]}
    | {object_reference_cycles, [[object_ref()]]}.

-type operation_error() ::
    {conflict, operation_conflict()}
    | {invalid, operation_invalid()}.

%% NOTE: The thrift-related types (struct_field_info, field_type, field_name)
%% are defined in `dmsl_domain_thrift` but not exported, so we mirror them
%% here. They are stable parts of the generated thrift surface.
-type struct_flavour() :: struct | exception | union.
-type field_name() :: atom().
-type field_type() ::
    bool
    | byte
    | i16
    | i32
    | i64
    | string
    | double
    | {enum, {module(), atom()}}
    | {struct, struct_flavour(), {module(), atom()}}
    | {list, field_type()}
    | {set, field_type()}
    | {map, field_type(), field_type()}.
-type struct_field_info() :: {pos_integer(), required | optional | undefined, field_type(), field_name(), term()}.
-type struct_info() :: {struct, struct_flavour(), [struct_field_info()]}.
-type schema_error() :: {error, {unknown_domain_object_tag, atom()}}.

-spec references(domain_object()) -> [object_ref()].
references(DomainObject) ->
    case get_data(DomainObject) of
        {error, _} ->
            [];
        {DataType, Data} ->
            references(Data, DataType)
    end.

-spec references(term(), field_type() | struct_info()) -> [object_ref()].
references(Object, DataType) ->
    references(Object, DataType, []).

-spec references(term(), field_type() | struct_info(), [object_ref()]) -> [object_ref()].
references(undefined, _StructInfo, Refs) ->
    Refs;
references({Tag, Object}, {struct, union, FieldsInfo} = StructInfo, Refs) when
    is_list(FieldsInfo)
->
    case get_field_info(Tag, StructInfo) of
        false ->
            erlang:error({<<"field info not found">>, Tag, StructInfo});
        {_, _, Type, _, _} ->
            check_reference_type(Object, Type, Refs)
    end;
%% what if it's a union?
references(Object, {struct, struct, FieldsInfo}, Refs) when is_list(FieldsInfo) ->
    indexfold(
        fun(I, {_, _Required, FieldType, _Name, _}, Acc) ->
            case element(I, Object) of
                undefined ->
                    Acc;
                Field ->
                    check_reference_type(Field, FieldType, Acc)
            end
        end,
        Refs,
        % NOTE
        % This `2` gives index of the first significant field in a record tuple.
        2,
        FieldsInfo
    );
references(Object, {struct, _, {?DOMAIN, StructName}}, Refs) ->
    StructInfo = get_struct_info(StructName),
    check_reference_type(Object, StructInfo, Refs);
references(Object, {list, FieldType}, Refs) ->
    lists:foldl(
        fun(O, Acc) ->
            check_reference_type(O, FieldType, Acc)
        end,
        Refs,
        Object
    );
references(Object, {set, FieldType}, Refs) ->
    ListObject = ordsets:to_list(Object),
    check_reference_type(ListObject, {list, FieldType}, Refs);
references(Object, {map, KeyType, ValueType}, Refs) ->
    maps:fold(
        fun(K, V, Acc) ->
            check_reference_type(
                V,
                ValueType,
                check_reference_type(K, KeyType, Acc)
            )
        end,
        Refs,
        Object
    );
references(_DomainObject, _Primitive, Refs) ->
    Refs.

-spec indexfold(fun((pos_integer(), Elem, Acc) -> Acc), Acc, pos_integer(), [Elem]) -> Acc.
indexfold(Fun, Acc, I, [E | Rest]) ->
    indexfold(Fun, Fun(I, E, Acc), I + 1, Rest);
indexfold(_Fun, Acc, _I, []) ->
    Acc.

-spec check_reference_type(term(), field_type() | struct_info(), [object_ref()]) -> [object_ref()].
check_reference_type(Object, Type, Refs) ->
    case is_reference_type(Type) of
        {true, Tag} ->
            [{Tag, Object} | Refs];
        false ->
            references(Object, Type, Refs)
    end.

-spec get_data(domain_object()) -> {field_type(), term()} | schema_error().
get_data(DomainObject) ->
    get_domain_object_field(data, DomainObject).

-spec get_domain_object_field(field_name(), domain_object()) ->
    {field_type(), term()} | schema_error().
get_domain_object_field(Field, {Tag, Struct}) ->
    case get_domain_object_schema(Tag) of
        {error, _} = Error ->
            Error;
        Schema ->
            get_field(Field, Struct, Schema)
    end.

-spec maybe_get_domain_object_data_field(field_name(), domain_object()) -> term() | undefined.
maybe_get_domain_object_data_field(Field, {Tag, Struct}) ->
    try get_data({Tag, Struct}) of
        {error, _} ->
            undefined;
        {_, Data} ->
            maybe_extract_field_from_data(Field, Tag, Data)
    catch
        Error:Reason:Stacktrace ->
            logger:warning("Error getting data field ~p for ~p: ~p", [
                Field, {Tag, Struct}, {Error, Reason, Stacktrace}
            ]),
            undefined
    end.

-spec maybe_extract_field_from_data(field_name(), atom(), tuple()) -> term() | undefined.
maybe_extract_field_from_data(Field, Tag, Data) ->
    SchemaInfo = get_struct_info('ReflessDomainObject'),
    case get_field_info(Tag, SchemaInfo) of
        {_, _, {struct, _, {_, ObjectStructName}}, _, _} ->
            maybe_get_field_by_index(Field, ObjectStructName, Data);
        false ->
            undefined
    end.

-spec maybe_get_field_by_index(field_name(), atom(), tuple()) -> term() | undefined.
maybe_get_field_by_index(Field, ObjectStructName, Data) ->
    DomainObjectSchema = get_struct_info(ObjectStructName),
    case get_field_index(Field, DomainObjectSchema) of
        {FieldIndex, _} ->
            element(FieldIndex, Data);
        false ->
            undefined
    end.

% limit_config is an exception, it's not in domain.thrift
-spec get_domain_object_schema(atom()) -> struct_info() | schema_error().
get_domain_object_schema(limit_config) ->
    dmsl_limiter_config_thrift:struct_info('LimitConfig');
get_domain_object_schema(Tag) ->
    SchemaInfo = get_struct_info('DomainObject'),
    case get_field_info(Tag, SchemaInfo) of
        {_, _, {struct, _, {_, ObjectStructName}}, _, _} ->
            get_struct_info(ObjectStructName);
        false ->
            {error, {unknown_domain_object_tag, Tag}}
    end.

-spec get_field(field_name(), tuple(), struct_info()) -> {field_type(), term()}.
get_field(Field, Struct, StructInfo) when is_atom(Field) ->
    {FieldIndex, {_, _, Type, _, _}} = get_field_index(Field, StructInfo),
    {Type, element(FieldIndex, Struct)}.

-spec get_struct_info(atom()) -> struct_info().
get_struct_info(StructName) ->
    dmsl_domain_thrift:struct_info(StructName).

-spec get_field_info(field_name(), struct_info()) -> struct_field_info() | false.
get_field_info(Field, {struct, _StructType, FieldsInfo}) ->
    lists:keyfind(Field, 4, FieldsInfo).

-spec get_field_index(field_name(), struct_info()) ->
    {pos_integer(), struct_field_info()} | false.
get_field_index(Field, {struct, _StructType, FieldsInfo}) ->
    % NOTE
    % This `2` gives index of the first significant field in a record tuple.
    get_field_index(Field, 2, FieldsInfo).

-spec get_field_index(field_name(), pos_integer(), [struct_field_info()]) ->
    {pos_integer(), struct_field_info()} | false.
get_field_index(_Field, _, []) ->
    false;
get_field_index(Field, I, [F | Rest]) ->
    case F of
        {_, _, _, Field, _} = Info ->
            {I, Info};
        _ ->
            get_field_index(Field, I + 1, Rest)
    end.

%% NOTE: `is_reference_type/1` is replaced at compile time by `dmt_domain_pt`
%% with a flat clause-per-reference-type function. The `is_reference_type/2`
%% helper below is removed by the same parse_transform, so it must remain
%% unspecced (a spec for it would be rejected as referring to an unknown function).
-spec is_reference_type(field_type() | struct_info()) -> {true, atom()} | false.
is_reference_type(Type) ->
    {struct, union, StructInfo} = get_struct_info('Reference'),
    is_reference_type(Type, StructInfo).

is_reference_type(_Type, []) ->
    false;
is_reference_type(Type, [{_, _, Type, Tag, _} | _Rest]) ->
    {true, Tag};
is_reference_type(Type, [_ | Rest]) ->
    is_reference_type(Type, Rest).
