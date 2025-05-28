-module(dmt_object_reference).

%% API

-export([get_domain_object_ref/1]).
-export([refless_object_references/1]).
-export([domain_object_references/1]).

-define(DOMAIN, dmsl_domain_thrift).

get_domain_object_ref({Tag, _Struct} = DomainObject) ->
    {_Type, Ref} = get_domain_object_field(ref, DomainObject),
    {Tag, Ref}.

%% RefflessObject ZONE

%% FIXME doesn't work
refless_object_references(DomainObject) ->
    {Data, DataType} = get_refless_data(DomainObject),
    references(Data, DataType).

get_refless_data({Tag, Struct}) ->
    {Struct, get_refless_object_schema(Tag)}.

get_refless_object_schema(Tag) ->
    SchemaInfo = get_struct_info('ReflessDomainObject'),
    case get_field_info(Tag, SchemaInfo) of
        {_, _, {struct, _, {_, ObjectStructName}}, _, _} ->
            {ObjectStructName, get_struct_info(ObjectStructName)};
        false ->
            erlang:error({field_info_not_found, Tag, SchemaInfo})
    end.

%% DomainObject ZONE

domain_object_references(DomainObject) ->
    {Data, DataType} = get_domain_object_data(DomainObject),
    references(Data, DataType).

get_domain_object_data(DomainObject) ->
    get_domain_object_field(data, DomainObject).

get_domain_object_field(Field, {Tag, Struct}) ->
    get_field(Field, Struct, get_domain_object_schema(Tag)).

get_domain_object_schema(Tag) ->
    SchemaInfo = get_struct_info('DomainObject'),
    {_, _, {struct, _, {_, ObjectStructName}}, _, _} = get_field_info(Tag, SchemaInfo),
    get_struct_info(ObjectStructName).

get_field(Field, Struct, StructInfo) when is_atom(Field) ->
    {FieldIndex, {_, _, Type, _, _}} = get_field_index(Field, StructInfo),
    {element(FieldIndex, Struct), Type}.

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

%% References Gathering ZONE

references(Object, DataType) ->
    references(Object, DataType, []).

references(undefined, _StructInfo, Refs) ->
    Refs;
references({Tag, Object}, {struct, union, FieldsInfo} = StructInfo, Refs) when is_list(FieldsInfo) ->
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

check_reference_type(Object, Type, Refs) ->
    case is_reference_type(Type) of
        {true, Tag} ->
            [{Tag, Object} | Refs];
        false ->
            references(Object, Type, Refs)
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

indexfold(Fun, Acc, I, [E | Rest]) ->
    indexfold(Fun, Fun(I, E, Acc), I + 1, Rest);
indexfold(_Fun, Acc, _I, []) ->
    Acc.

%% Common

get_struct_info('LimitConfig') ->
    dmsl_limiter_config_thrift:struct_info('LimitConfig');
get_struct_info(StructName) ->
    dmsl_domain_thrift:struct_info(StructName).

get_field_info(Field, {struct, _StructType, FieldsInfo}) ->
    lists:keyfind(Field, 4, FieldsInfo).
