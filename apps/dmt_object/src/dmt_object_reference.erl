-module(dmt_object_reference).

%% API

-export([get_domain_object_ref/1]).
-export([refless_object_references/1]).
-export([domain_object_references/1]).

-define(DOMAIN, dmsl_domain_thrift).

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
-type struct_name() :: atom().
-type domain_reference() :: dmsl_domain_thrift:'Reference'().

-export_type([domain_reference/0]).

-spec get_domain_object_ref(dmsl_domain_thrift:'DomainObject'()) -> domain_reference().
get_domain_object_ref({Tag, _Struct} = DomainObject) ->
    {_Type, Ref} = get_domain_object_field(ref, DomainObject),
    {Tag, Ref}.

%% RefflessObject ZONE

%% FIXME doesn't work
-spec refless_object_references(dmsl_domain_thrift:'ReflessDomainObject'()) -> [domain_reference()].
refless_object_references(DomainObject) ->
    {Data, DataType} = get_refless_data(DomainObject),
    references(Data, DataType).

-spec get_refless_data(dmsl_domain_thrift:'ReflessDomainObject'()) -> {tuple(), field_type()}.
get_refless_data({Tag, Struct}) ->
    SchemaInfo = get_struct_info('ReflessDomainObject'),
    case get_field_info(Tag, SchemaInfo) of
        {_, _, DataType, _, _} ->
            {Struct, DataType};
        false ->
            erlang:error({field_info_not_found, Tag, SchemaInfo})
    end.

%% DomainObject ZONE

-spec domain_object_references(dmsl_domain_thrift:'DomainObject'()) -> [domain_reference()].
domain_object_references(DomainObject) ->
    {Data, DataType} = get_domain_object_data(DomainObject),
    references(Data, DataType).

-spec get_domain_object_data(dmsl_domain_thrift:'DomainObject'()) -> {term(), field_type()}.
get_domain_object_data(DomainObject) ->
    get_domain_object_field(data, DomainObject).

-spec get_domain_object_field(field_name(), dmsl_domain_thrift:'DomainObject'()) ->
    {term(), field_type()}.
get_domain_object_field(Field, {Tag, Struct}) ->
    get_field(Field, Struct, get_domain_object_schema(Tag)).

-spec get_domain_object_schema(atom()) -> struct_info().
get_domain_object_schema(Tag) ->
    SchemaInfo = get_struct_info('DomainObject'),
    {_, _, {struct, _, {_, ObjectStructName}}, _, _} = get_field_info(Tag, SchemaInfo),
    get_struct_info(ObjectStructName).

-spec get_field(field_name(), tuple(), struct_info()) -> {term(), field_type()}.
get_field(Field, Struct, StructInfo) when is_atom(Field) ->
    {FieldIndex, {_, _, Type, _, _}} = get_field_index(Field, StructInfo),
    {element(FieldIndex, Struct), Type}.

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

%% References Gathering ZONE

-spec references(term(), field_type() | struct_info()) -> [domain_reference()].
references(Object, DataType) ->
    references(Object, DataType, []).

-spec references(term(), field_type() | struct_info(), [domain_reference()]) -> [domain_reference()].
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

-spec check_reference_type(term(), field_type() | struct_info(), [domain_reference()]) -> [domain_reference()].
check_reference_type(Object, Type, Refs) ->
    case is_reference_type(Type) of
        {true, Tag} ->
            [{Tag, Object} | Refs];
        false ->
            references(Object, Type, Refs)
    end.

-spec is_reference_type(field_type() | struct_info()) -> {true, atom()} | false.
is_reference_type(Type) ->
    {struct, union, StructInfo} = get_struct_info('Reference'),
    is_reference_type(Type, StructInfo).

-spec is_reference_type(field_type() | struct_info(), [struct_field_info()]) ->
    {true, atom()} | false.
is_reference_type(_Type, []) ->
    false;
is_reference_type(Type, [{_, _, Type, Tag, _} | _Rest]) ->
    {true, Tag};
is_reference_type(Type, [_ | Rest]) ->
    is_reference_type(Type, Rest).

-spec indexfold(fun((pos_integer(), Elem, Acc) -> Acc), Acc, pos_integer(), [Elem]) -> Acc.
indexfold(Fun, Acc, I, [E | Rest]) ->
    indexfold(Fun, Fun(I, E, Acc), I + 1, Rest);
indexfold(_Fun, Acc, _I, []) ->
    Acc.

%% Common

-spec get_struct_info(struct_name()) -> struct_info().
get_struct_info('LimitConfig') ->
    dmsl_limiter_config_thrift:struct_info('LimitConfig');
get_struct_info(StructName) ->
    dmsl_domain_thrift:struct_info(StructName).

-spec get_field_info(field_name(), struct_info()) -> struct_field_info() | false.
get_field_info(Field, {struct, _StructType, FieldsInfo}) ->
    lists:keyfind(Field, 4, FieldsInfo).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").
-include_lib("damsel/include/dmsl_domain_thrift.hrl").

-spec test() -> _.

-spec reference_extraction_test() -> _.
reference_extraction_test() ->
    ProxyRef = #domain_ProxyRef{
        id = 1
    },
    ProviderObject =
        {provider, #domain_Provider{
            name = <<"referencing_provider">>,
            realm = test,
            description = <<"provider that references the proxy">>,
            proxy = #domain_Proxy{
                ref = ProxyRef,
                additional = #{}
            }
        }},

    _ = ?assertEqual([{proxy, ProxyRef}], refless_object_references(ProviderObject)).

-spec domain_object_reference_extraction_test() -> _.
domain_object_reference_extraction_test() ->
    ProxyRef = #domain_ProxyRef{
        id = 1
    },
    ProviderObject =
        {provider, #domain_ProviderObject{
            ref = #domain_ProviderRef{id = 1},
            data =
                #domain_Provider{
                    name = <<"referencing_provider">>,
                    realm = test,
                    description = <<"provider that references the proxy">>,
                    proxy = #domain_Proxy{
                        ref = ProxyRef,
                        additional = #{}
                    }
                }
        }},

    _ = ?assertEqual([{proxy, ProxyRef}], domain_object_references(ProviderObject)).

-endif.
