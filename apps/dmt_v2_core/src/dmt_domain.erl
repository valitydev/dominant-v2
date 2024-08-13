-module(dmt_domain).

-include_lib("damsel/include/dmsl_domain_conf_thrift.hrl").
-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

-compile({parse_transform, dmt_domain_pt}).

%%

-export([references/1]).

-define(DOMAIN, dmsl_domain_thrift).

-export_type([operation_error/0]).

%%

-type operation() :: dmsl_domain_conf_thrift:'Operation'().
-type object_ref() :: dmsl_domain_thrift:'Reference'().
-type domain() :: dmsl_domain_thrift:'Domain'().
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

-type fold_function() :: fun((object_ref(), domain_object(), AccIn :: term()) -> AccOut :: term()).

-spec get_object(object_ref(), domain()) -> {ok, domain_object()} | error.
get_object(ObjectReference, Domain) ->
    maps:find(ObjectReference, Domain).

-spec apply_operations([operation()], domain()) -> {ok, domain()} | {error, operation_error()}.
apply_operations(Operations, Domain) ->
    apply_operations(Operations, Domain, []).

apply_operations([], Domain, Touched) ->
    case integrity_check(Domain, lists:reverse(Touched)) of
        ok ->
            {ok, Domain};
        {error, Invalid} ->
            {error, {invalid, Invalid}}
    end;
apply_operations(
    [Op | Rest],
    TransactionPrep,
    Touched
) ->
    {Result, Touch} =
        case Op of
            {insert, #domain_conf_v2_InsertOp{object = Object, force_ref = DesiredRef}} ->
                {insert(Object, DesiredRef), {insert, Object}};
            {update, #domain_conf_v2_UpdateOp{targeted_ref = OldObject, new_object = NewObject}} ->
                {update(OldObject, NewObject, Domain), {update, NewObject}};
            {remove, #domain_conf_v2_RemoveOp{ref = Object}} ->
                {remove(Object, Domain), {remove, Object}}
        end,
    case Result of
        {ok, NewDomain} ->
            apply_operations(Rest, NewDomain, [Touch | Touched]);
        {error, Conflict} ->
            {error, {conflict, Conflict}}
    end.

-spec insert(domain_object(), domain()) ->
    {ok, domain()}
    | {error, {object_already_exists, object_ref()}}.
insert(Object, Domain) ->
    ObjectReference = get_ref(Object),
    case maps:find(ObjectReference, Domain) of
        error ->
            {ok, maps:put(ObjectReference, Object, Domain)};
        {ok, ObjectWas} ->
            {error, {object_already_exists, get_ref(ObjectWas)}}
    end.

-spec update(domain_object(), domain_object(), domain()) ->
    {ok, domain()}
    | {error,
        {object_not_found, object_ref()}
        | {object_reference_mismatch, object_ref()}}.
update(OldObject, NewObject, Domain) ->
    ObjectReference = get_ref(OldObject),
    case get_ref(NewObject) of
        ObjectReference ->
            case maps:find(ObjectReference, Domain) of
                {ok, OldObject} ->
                    {ok, maps:put(ObjectReference, NewObject, Domain)};
                {ok, _ObjectWas} ->
                    {error, {object_not_found, ObjectReference}};
                error ->
                    {error, {object_not_found, ObjectReference}}
            end;
        NewObjectReference ->
            {error, {object_reference_mismatch, NewObjectReference}}
    end.

-spec remove(domain_object(), domain()) ->
    {ok, domain()}
    | {error, {object_not_found, object_ref()}}.
remove(Object, Domain) ->
    ObjectReference = get_ref(Object),
    case maps:find(ObjectReference, Domain) of
        {ok, Object} ->
            {ok, maps:remove(ObjectReference, Domain)};
        {ok, _ObjectWas} ->
            {error, {object_not_found, ObjectReference}};
        error ->
            {error, {object_not_found, ObjectReference}}
    end.

-type touch() :: {insert | update | remove, domain_object()}.

-spec integrity_check(domain(), [touch()]) ->
    ok
    | {error,
        {objects_not_exist, [nonexistent_object()]}
        | {object_reference_cycles, [[object_ref()]]}}.
integrity_check(Domain, Touched) when is_list(Touched) ->
    % TODO
    % Well I guess nothing (but the types) stops us from accumulating
    % errors from every check, instead of just first failed
    run_until_error([
        fun() -> verify_integrity(Domain, Touched) end,
        fun() -> verify_acyclicity(Domain, Touched, {[], #{}}) end
    ]).

run_until_error([CheckFun | Rest]) ->
    case CheckFun() of
        ok ->
            run_until_error(Rest);
        {error, _} = Error ->
            Error
    end;
run_until_error([]) ->
    ok.

verify_integrity(Domain, Touched) ->
    ObjectsNotExist1 = verify_forward_integrity(Domain, Touched, []),
    ObjectsNotExist2 = verify_backward_integrity(Domain, Touched, ObjectsNotExist1),
    case ObjectsNotExist2 of
        [] ->
            ok;
        [_ | _] ->
            {error, {objects_not_exist, ObjectsNotExist2}}
    end.

verify_forward_integrity(Domain, Ops, ObjectsNotExistAcc) ->
    lists:foldl(
        fun
            ({Op, Object}, Acc) when Op == insert; Op == update ->
                Acc ++ check_correct_refs(Object, Domain);
            (_, Acc) ->
                Acc
        end,
        ObjectsNotExistAcc,
        Ops
    ).

verify_backward_integrity(Domain, Ops, ObjectsNotExistAcc) ->
    RemovedRefs = [get_ref(Object) || {remove, Object} <- Ops],
    ObjectsNotExistAcc ++ check_no_refs(RemovedRefs, Domain).

verify_acyclicity(_Domain, [], {[], _}) ->
    ok;
verify_acyclicity(_Domain, [], {Cycles, _}) ->
    {error, {object_reference_cycles, Cycles}};
verify_acyclicity(Domain, [{Op, Object} | Rest], Acc) when Op == insert; Op == update ->
    Acc1 = track_cycles_from(get_ref(Object), Object, Acc, Domain),
    verify_acyclicity(Domain, Rest, Acc1);
verify_acyclicity(Domain, [{remove, _} | Rest], Acc) ->
    verify_acyclicity(Domain, Rest, Acc).

check_correct_refs(DomainObject, Domain) ->
    NonExistent = lists:filter(
        fun(E) ->
            not object_exists(E, Domain)
        end,
        references(DomainObject)
    ),
    Ref = get_ref(DomainObject),
    lists:map(fun(X) -> {X, [Ref]} end, NonExistent).

object_exists(Ref, Domain) ->
    case get_object(Ref, Domain) of
        {ok, _Object} ->
            true;
        error ->
            false
    end.

check_no_refs(Refs, Domain) ->
    case maps:to_list(referenced_by(Refs, Domain)) of
        [] ->
            [];
        ReferencedBy ->
            ReferencedBy
    end.

track_cycles_from(Ref, Object, {Acc, Blocklist}, Domain) ->
    %% NOTE
    %%
    %% This is an implementation of [Johnson's algorithm for enumerating
    %% elementary cycles of a directed graph][1], simplified and adapted
    %% for the functional setting. As far as we're aware it's best known
    %% algorithm for the problem in terms of complexity, however, it's not
    %% entirely clear how badly complexity was affected by our simplifications.
    %%
    %% The first simplification is as follows. Instead of iterating over strongly
    %% connected components of a whole graph we instead iterate only over
    %% those nodes which were affected by the last operation, one by one.
    %% During each iteration we enumerate _all_ cycles passing through a node,
    %% then remove this node (and all in-edges effectively) from the graph, so
    %% that we will no longer track this node in following iterations.
    %%
    %% The second simplification stems from the observation that blocked-by
    %% relationships explicitly tracked with `B(v)` in the paper are in fact
    %% implicitly tracked with an execution stack, when implemented
    %% functionally.
    %%
    %% We assume that taking into account only last affected (added or updated)
    %% nodes is safe by induction: obviously, there are no cycles in an empty
    %% graph, and no operation introducing a cycle could succeed.
    %%
    %% [1]: https://www.cs.tufts.edu/comp/150GA/homeworks/hw1/Johnson%2075.PDF
    {_Found, Acc1, _Blocklist} = track_cycles_over(Object, Ref, [Ref], Acc, Blocklist, Domain),
    {Acc1, block_node(Ref, Blocklist)}.

track_cycles_over(DomainObject, Pivot, [Ref | _] = PathRev, Acc, Blocklist, Domain) ->
    %% NOTE
    %% Returns _all_ cycles passing through the node `Pivot`.
    %% This is essentially CIRCUIT(v) routine from the [paper][1].
    Refs = references(DomainObject),
    %% We begin by blocking current node so that any search passing through
    %% this node (but not through `Pivot`) would terminate prematurely.
    Blocklist1 = block_node(Ref, Blocklist),
    {Found, Acc1, Blocklist2} = lists:foldl(
        fun(NextRef, {FAcc, CAcc, BLAcc}) ->
            %% We iterate over adjacent nodes here, accumulating cycles and
            %% blocklist. If _any_ cycle is found in any subgraph then `Found`
            %% will become `true`.
            track_edge(NextRef, Pivot, PathRev, FAcc, CAcc, BLAcc, Domain)
        end,
        {false, Acc, Blocklist1},
        Refs
    ),
    Blocklist3 =
        case Found of
            true ->
                %% We found a cycle. Great but now we have to unblock current node.
                %% At this point it looks safe to assume that all of its ascendants
                %% will be unblocked eventually while stack is unwinding.
                unblock_node(Ref, Blocklist2);
            false ->
                Blocklist2
        end,
    {Found, Acc1, Blocklist3}.

track_edge(Ref, Ref, PathRev, _Found, Acc, Blocklist, _Domain) ->
    % We found a cycle passing through `Pivot`.
    % That means we must return with `true` to a caller.
    Acc1 = [lists:reverse(PathRev) | Acc],
    {true, Acc1, Blocklist};
track_edge(Ref, Pivot, PathRev, Found, Acc, Blocklist, Domain) ->
    case is_blocked(Ref, Blocklist) of
        true ->
            % Node is blocked.
            % Either this is a dead end so there's no reason to go there, or
            % this is a cycle not passing through `Pivot` and we'll find it
            % eventually but later.
            {Found, Acc, Blocklist};
        false ->
            % First time here.
            case get_object(Ref, Domain) of
                {ok, Object} ->
                    track_cycles_over(Object, Pivot, [Ref | PathRev], Acc, Blocklist, Domain);
                error ->
                    {Found, Acc, Blocklist}
            end
    end.

block_node(Ref, Blocklist) ->
    Blocklist#{Ref => true}.

is_blocked(Ref, Blocklist) ->
    maps:get(Ref, Blocklist, false).

unblock_node(Ref, Blocklist) ->
    maps:remove(Ref, Blocklist).

referenced_by(Refs, Domain) ->
    RefSet = ordsets:from_list(Refs),
    maps:fold(
        fun(K, V, Acc0) ->
            OutRefSet = ordsets:from_list(references(V)),
            Intersection = ordsets:intersection(RefSet, OutRefSet),
            ordsets:fold(
                fun(Ref, Acc) -> map_append(Ref, K, Acc) end,
                Acc0,
                Intersection
            )
        end,
        #{},
        Domain
    ).

map_append(K, V, M) ->
    maps:put(K, [V | maps:get(K, M, [])], M).

references(DomainObject) ->
    {DataType, Data} = get_data(DomainObject),
    references(Data, DataType).

references(Object, DataType) ->
    references(Object, DataType, []).

references(undefined, _StructInfo, Refs) ->
    Refs;
references({Tag, Object}, StructInfo = {struct, union, FieldsInfo}, Refs) when is_list(FieldsInfo) ->
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

-spec get_ref(domain_object()) -> object_ref().
get_ref(DomainObject = {Tag, _Struct}) ->
    {_Type, Ref} = get_domain_object_field(ref, DomainObject),
    {Tag, Ref}.

-spec get_data(domain_object()) -> any().
get_data(DomainObject) ->
    get_domain_object_field(data, DomainObject).

get_domain_object_field(Field, {Tag, Struct}) ->
    get_field(Field, Struct, get_domain_object_schema(Tag)).

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

invert_operation({insert, #domain_conf_InsertOp{object = Object}}) ->
    {remove, #domain_conf_RemoveOp{object = Object}};
invert_operation({update, #domain_conf_UpdateOp{old_object = OldObject, new_object = NewObject}}) ->
    {update, #domain_conf_UpdateOp{old_object = NewObject, new_object = OldObject}};
invert_operation({remove, #domain_conf_RemoveOp{object = Object}}) ->
    {insert, #domain_conf_InsertOp{object = Object}}.
