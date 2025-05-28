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

references(DomainObject) ->
    {DataType, Data} = get_data(DomainObject),
    references(Data, DataType).

references(Object, DataType) ->
    references(Object, DataType, []).

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

indexfold(Fun, Acc, I, [E | Rest]) ->
    indexfold(Fun, Fun(I, E, Acc), I + 1, Rest);
indexfold(_Fun, Acc, _I, []) ->
    Acc.

check_reference_type(Object, Type, Refs) ->
    case is_reference_type(Type) of
        {true, Tag} ->
            [{Tag, Object} | Refs];
        false ->
            references(Object, Type, Refs)
    end.

-spec get_data(domain_object()) -> any().
get_data(DomainObject) ->
    get_domain_object_field(data, DomainObject).

get_domain_object_field(Field, {Tag, Struct}) ->
    get_field(Field, Struct, get_domain_object_schema(Tag)).

maybe_get_domain_object_data_field(Field, {Tag, Struct}) ->
    {_, Data} = get_data({Tag, Struct}),
    SchemaInfo = get_struct_info('ReflessDomainObject'),
    {_, _, {struct, _, {_, ObjectStructName}}, _, _} = get_field_info(Tag, SchemaInfo),
    DomainObjectSchema = get_struct_info(ObjectStructName),
    case get_field_index(Field, DomainObjectSchema) of
        {FieldIndex, _} ->
            element(FieldIndex, Data);
        false ->
            undefined
    end.

get_domain_object_schema(Tag) ->
    SchemaInfo = get_struct_info('DomainObject'),
    {_, _, {struct, _, {_, ObjectStructName}}, _, _} = get_field_info(Tag, SchemaInfo),
    get_struct_info(ObjectStructName).

get_field(Field, Struct, StructInfo) when is_atom(Field) ->
    {FieldIndex, {_, _, Type, _, _}} = get_field_index(Field, StructInfo),
    {Type, element(FieldIndex, Struct)}.

get_struct_info(StructName) ->
    dmsl_domain_thrift:struct_info(StructName).

get_field_info(Field, {struct, _StructType, FieldsInfo}) ->
    lists:keyfind(Field, 4, FieldsInfo).

get_field_index(Field, {struct, _StructType, FieldsInfo}) ->
    % NOTE
    % This `2` gives index of the first significant field in a record tuple.
    get_field_index(Field, 2, FieldsInfo).

get_field_index(_Field, _, []) ->
    false;
get_field_index(Field, I, [F | Rest]) ->
    case F of
        {_, _, _, Field, _} = Info ->
            {I, Info};
        _ ->
            get_field_index(Field, I + 1, Rest)
    end.

is_reference_type(Type) ->
    {struct, union, StructInfo} = get_struct_info('Reference'),
    is_reference_type(Type, StructInfo).

is_reference_type(_Type, []) ->
    false;
is_reference_type(Type, [{_, _, Type, Tag, _} | _Rest]) ->
    {true, Tag};
is_reference_type(Type, [_ | Rest]) ->
    is_reference_type(Type, Rest).
