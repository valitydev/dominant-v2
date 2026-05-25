-module(dmt_object_reference).

%% API

-export([get_domain_object_ref/1]).
-export([refless_object_references/1]).
-export([domain_object_references/1]).

-define(DOMAIN, dmsl_domain_thrift).

-type object_type() :: dmt_object:object_type().
-type object_reference() :: {object_type(), term()}.
-type thrift_type() :: dmt_thrift:thrift_type().
-type struct_info() :: dmt_thrift:struct_info().
-type field_info() :: dmt_thrift:field_info().

-spec get_domain_object_ref(tuple()) -> object_reference().
get_domain_object_ref({Tag, _Struct} = DomainObject) ->
    {_Type, Ref} = get_domain_object_field(ref, DomainObject),
    {Tag, Ref}.

%% RefflessObject ZONE

%% FIXME doesn't work
-spec refless_object_references(tuple()) -> [object_reference()].
refless_object_references(DomainObject) ->
    {Data, DataType} = get_refless_data(DomainObject),
    references(Data, DataType).

-spec get_refless_data(tuple()) -> {eqwalizer:dynamic(), thrift_type()}.
get_refless_data({Tag, Struct}) ->
    SchemaInfo = get_struct_info('ReflessDomainObject'),
    case get_field_info(Tag, SchemaInfo) of
        {_, _, DataType, _, _} ->
            {Struct, DataType};
        false ->
            erlang:error({field_info_not_found, Tag, SchemaInfo})
    end.

%% DomainObject ZONE

-spec domain_object_references(tuple()) -> [object_reference()].
domain_object_references(DomainObject) ->
    {Data, DataType} = get_domain_object_data(DomainObject),
    references(Data, DataType).

-spec get_domain_object_data(tuple()) -> {eqwalizer:dynamic(), thrift_type()}.
get_domain_object_data(DomainObject) ->
    get_domain_object_field(data, DomainObject).

-spec get_domain_object_field(atom(), tuple()) -> {eqwalizer:dynamic(), thrift_type()}.
get_domain_object_field(Field, {Tag, Struct}) ->
    get_field(Field, Struct, get_domain_object_schema(Tag)).

-spec get_domain_object_schema(atom()) -> struct_info().
get_domain_object_schema(Tag) ->
    SchemaInfo = get_struct_info('DomainObject'),
    {_, _, {struct, _, {_, ObjectStructName}}, _, _} = get_field_info(Tag, SchemaInfo),
    get_struct_info(ObjectStructName).

-spec get_field(atom(), tuple(), struct_info()) -> {eqwalizer:dynamic(), thrift_type()}.
get_field(Field, Struct, StructInfo) when is_atom(Field) ->
    {FieldIndex, {_, _, Type, _, _}} = get_field_index(Field, StructInfo),
    {element(FieldIndex, Struct), Type}.

-spec get_field_index(atom(), struct_info()) -> {pos_integer(), field_info()} | false.
get_field_index(Field, {struct, _StructType, FieldsInfo}) ->
    % NOTE
    % This `2` gives index of the first significant field in a record tuple.
    get_field_index(Field, 2, FieldsInfo).

-spec get_field_index(atom(), pos_integer(), [field_info()]) ->
    {pos_integer(), field_info()} | false.
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

-spec references(eqwalizer:dynamic(), thrift_type()) -> [object_reference()].
references(Object, DataType) ->
    references(Object, DataType, []).

-spec references(eqwalizer:dynamic(), thrift_type(), [object_reference()]) ->
    [object_reference()].
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

-spec check_reference_type(eqwalizer:dynamic(), thrift_type(), [object_reference()]) ->
    [object_reference()].
check_reference_type(Object, Type, Refs) ->
    case is_reference_type(Type) of
        {true, Tag} ->
            [{Tag, Object} | Refs];
        false ->
            references(Object, Type, Refs)
    end.

-spec is_reference_type(thrift_type()) -> {true, atom()} | false.
is_reference_type(Type) ->
    case get_struct_info('Reference') of
        {struct, union, StructInfo} when is_list(StructInfo) ->
            is_reference_type(Type, StructInfo);
        _ ->
            false
    end.

-spec is_reference_type(thrift_type(), [field_info()]) -> {true, atom()} | false.
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

-spec get_struct_info(atom()) -> struct_info().
get_struct_info('LimitConfig') ->
    %% Cast: each `dmsl_*_thrift` module exports its own nominal `struct_info()`
    %% type. Ours is the structurally identical local alias — eqwalizer treats
    %% the two as distinct nominal types so the cross-module boundary needs an
    %% explicit cast.
    eqwalizer:dynamic_cast(dmsl_limiter_config_thrift:struct_info('LimitConfig'));
get_struct_info(StructName) ->
    %% Outer cast: same nominal-vs-structural reason as the `LimitConfig` clause.
    %% Inner cast on `StructName`: `dmsl_domain_thrift:struct_info/1` expects a
    %% specific atom union (`struct_name() | exception_name()`); the value here
    %% comes from runtime schema lookups so we only know it's an `atom()`.
    eqwalizer:dynamic_cast(
        dmsl_domain_thrift:struct_info(eqwalizer:dynamic_cast(StructName))
    ).

-spec get_field_info(atom(), struct_info()) -> field_info() | false.
get_field_info(Field, {struct, _StructType, FieldsInfo}) ->
    case lists:keyfind(Field, 4, FieldsInfo) of
        false -> false;
        T -> T
    end.

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
