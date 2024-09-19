-module(dmt_v2_object).

-feature(maybe_expr, enable).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

-export([new_object/1]).
-export([update_object/2]).
-export([remove_object/1]).
-export([just_object/7]).

-export_type([insertable_object/0]).
-export_type([object_changes/0]).
-export_type([object/0]).

-type object_type() :: atom().
-type object_id() :: string().
-type timestamp() :: string().

-type insertable_object() :: #{
    type := object_type(),
    is_id_generatable := boolean(),
    id_generator := string() | undefined,
    forced_id := string() | undefined,
    references := [{object_type(), object_id()}],
    data := binary()
}.

-type object_changes() :: #{
    id := object_id(),
    type := object_type(),
    references => [{object_type(), object_id()}],
    referenced_by => [{object_type(), object_id()}],
    data => binary(),
    is_active => boolean()
}.

-type object() :: #{
    id := object_id(),
    type := object_type(),
    global_version := string(),
    references := [{object_type(), object_id()}],
    referenced_by := [{object_type(), object_id()}],
    data := binary(),
    created_at := timestamp(),
    created_by := string()
}.

new_object(#domain_conf_v2_InsertOp{
    object = NewObject,
    force_ref = ForcedRef
}) ->
    case get_checked_type(ForcedRef, NewObject) of
        {ok, Type} ->
            {ok, #{
                tmp_id => uuid:get_v4_urandom(),
                type => Type,
                is_id_generatable => dmt_v2_object_id:is_id_generatable(Type),
                id_generator => dmt_v2_object_id:id_generator(Type),
                forced_id => term_to_binary(ForcedRef),
                references => list_term_to_binary(dmt_v2_object_reference:refless_object_references(NewObject)),
                data => term_to_binary(NewObject)
            }};
        {error, Error} ->
            {error, Error}
    end.

update_object(
    #domain_conf_v2_UpdateOp{
        targeted_ref = TargetedRef,
        new_object = NewObject
    },
    ExistingUpdate
) ->
    maybe
        {ok, Type} ?= get_checked_type(TargetedRef, NewObject),
        ok ?= check_domain_object_refs(TargetedRef, NewObject),
        {ok, ExistingUpdate#{
            id => TargetedRef,
            type => Type,
            %%          NOTE this will just provide all the refs that already exist,
            %%          it doesn't give us diff, but maybe it's not needed
            references => dmt_v2_object_reference:domain_object_references(NewObject),
            data => jsx:encode(NewObject)
        }}
    end.

remove_object(#domain_conf_v2_RemoveOp{ref = Ref}) ->
    {Type, _} = Ref,
    {ok, #{
        id => Ref,
        type => Type,
        is_active => false
    }}.

just_object(
    ID,
    Version,
    ReferencesTo,
    ReferencedBy,
    Data,
    CreatedAt,
    CreatedBy
) ->
    {Type, _} = ID,
    #{
        id => ID,
        type => Type,
        global_version => Version,
        references => ReferencesTo,
        referenced_by => ReferencedBy,
        data => Data,
        created_at => CreatedAt,
        created_by => CreatedBy
    }.

get_checked_type(undefined, {Type, _}) ->
    {ok, Type};
get_checked_type({Type, _}, {Type, _}) ->
    {ok, Type};
get_checked_type(Ref, Object) ->
    {error, {type_mismatch, Ref, Object}}.

check_domain_object_refs(Ref, Object) ->
    case dmt_v2_object_reference:get_domain_object_ref(Object) of
        Ref ->
            ok;
        _ ->
            {error, {reference_mismatch, Ref, Object}}
    end.

list_term_to_binary(Terms) ->
    lists:map(fun (Term) -> term_to_binary(Term) end, Terms).
