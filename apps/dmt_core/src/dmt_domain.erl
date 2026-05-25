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
-export_type([thrift_type/0]).
-export_type([object_reference/0]).

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

-type thrift_type() :: dmt_thrift:thrift_type().
-type struct_info() :: dmt_thrift:struct_info().
-type field_info() :: dmt_thrift:field_info().

-type object_reference() :: {atom(), term()}.

-spec references(domain_object()) -> [object_reference()].
references(DomainObject) ->
    case get_data(DomainObject) of
        {error, _} ->
            [];
        {DataType, Data} ->
            references(Data, DataType)
    end.

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

-spec indexfold(fun((pos_integer(), Elem, Acc) -> Acc), Acc, pos_integer(), [Elem]) -> Acc.
indexfold(Fun, Acc, I, [E | Rest]) ->
    indexfold(Fun, Fun(I, E, Acc), I + 1, Rest);
indexfold(_Fun, Acc, _I, []) ->
    Acc.

-spec check_reference_type(eqwalizer:dynamic(), thrift_type(), [object_reference()]) ->
    [object_reference()].
check_reference_type(Object, Type, Refs) ->
    case is_reference_type(Type) of
        {true, Tag} ->
            [{Tag, Object} | Refs];
        false ->
            references(Object, Type, Refs)
    end.

-spec get_data(domain_object()) ->
    {thrift_type(), eqwalizer:dynamic()} | {error, {unknown_domain_object_tag, atom()}}.
get_data(DomainObject) ->
    get_domain_object_field(data, DomainObject).

-spec get_domain_object_field(atom(), domain_object()) ->
    {thrift_type(), eqwalizer:dynamic()} | {error, {unknown_domain_object_tag, atom()}}.
get_domain_object_field(Field, {Tag, Struct}) ->
    case get_domain_object_schema(Tag) of
        {error, _} = Error ->
            Error;
        Schema ->
            get_field(Field, Struct, Schema)
    end.

-spec maybe_get_domain_object_data_field(atom(), domain_object()) ->
    eqwalizer:dynamic() | undefined.
maybe_get_domain_object_data_field(Field, {Tag, _Struct} = DomainObject) ->
    try get_data(DomainObject) of
        {error, _} ->
            undefined;
        {_, Data} ->
            maybe_extract_field_from_data(Field, Tag, Data)
    catch
        Error:Reason:Stacktrace ->
            logger:warning("Error getting data field ~p for ~p: ~p", [
                Field, DomainObject, {Error, Reason, Stacktrace}
            ]),
            undefined
    end.

-spec maybe_extract_field_from_data(atom(), atom(), eqwalizer:dynamic()) ->
    eqwalizer:dynamic() | undefined.
maybe_extract_field_from_data(Field, Tag, Data) ->
    SchemaInfo = get_struct_info('ReflessDomainObject'),
    case get_field_info(Tag, SchemaInfo) of
        {_, _, {struct, _, {_, ObjectStructName}}, _, _} ->
            maybe_get_field_by_index(Field, ObjectStructName, Data);
        false ->
            undefined
    end.

-spec maybe_get_field_by_index(atom(), atom(), tuple()) -> eqwalizer:dynamic() | undefined.
maybe_get_field_by_index(Field, ObjectStructName, Data) ->
    DomainObjectSchema = get_struct_info(ObjectStructName),
    case get_field_index(Field, DomainObjectSchema) of
        {FieldIndex, _} ->
            element(FieldIndex, Data);
        false ->
            undefined
    end.

% limit_config is an exception, it's not in domain.thrift
-spec get_domain_object_schema(atom()) ->
    struct_info() | {error, {unknown_domain_object_tag, atom()}}.
get_domain_object_schema(limit_config) ->
    %% Cast: `dmsl_limiter_config_thrift` exports its own nominal `struct_info()`
    %% type. Our local `struct_info()` is the structurally identical alias —
    %% eqwalizer treats the two as distinct nominal types so we need to cross
    %% the cross-thrift-module boundary explicitly here.
    eqwalizer:dynamic_cast(dmsl_limiter_config_thrift:struct_info('LimitConfig'));
get_domain_object_schema(Tag) ->
    SchemaInfo = get_struct_info('DomainObject'),
    case get_field_info(Tag, SchemaInfo) of
        {_, _, {struct, _, {_, ObjectStructName}}, _, _} when is_atom(ObjectStructName) ->
            get_struct_info(ObjectStructName);
        _ ->
            {error, {unknown_domain_object_tag, Tag}}
    end.

-spec get_field(atom(), tuple(), struct_info()) -> {thrift_type(), eqwalizer:dynamic()}.
get_field(Field, Struct, StructInfo) when is_atom(Field) ->
    {FieldIndex, {_, _, Type, _, _}} = get_field_index(Field, StructInfo),
    {Type, element(FieldIndex, Struct)}.

-spec get_struct_info(atom()) -> struct_info().
get_struct_info(StructName) ->
    %% Outer cast: `dmsl_domain_thrift:struct_info/1` returns its nominal
    %% `struct_info()` type which is structurally identical to but nominally
    %% distinct from our local `struct_info()` alias.
    %% Inner cast on `StructName`: the callee expects a specific atom union
    %% (`struct_name() | exception_name()`); the value comes from a runtime
    %% schema field so we only know it's an `atom()`.
    eqwalizer:dynamic_cast(
        dmsl_domain_thrift:struct_info(eqwalizer:dynamic_cast(StructName))
    ).

-spec get_field_info(atom(), struct_info()) -> field_info() | false.
get_field_info(Field, {struct, _StructType, FieldsInfo}) ->
    case lists:keyfind(Field, 4, FieldsInfo) of
        false -> false;
        T -> T
    end.

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

-spec is_reference_type(thrift_type()) -> {true, atom()} | false.
is_reference_type(Type) ->
    case get_struct_info('Reference') of
        {struct, union, StructInfo} when is_list(StructInfo) ->
            is_reference_type(Type, StructInfo);
        _ ->
            false
    end.

%% NOTE: dmt_domain_pt parse_transform removes is_reference_type/2 — no spec.
is_reference_type(_Type, []) ->
    false;
is_reference_type(Type, [{_, _, Type, Tag, _} | _Rest]) ->
    {true, Tag};
is_reference_type(Type, [_ | Rest]) ->
    is_reference_type(Type, Rest).
