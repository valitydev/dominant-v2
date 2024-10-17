-module(dmt_object).

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
    tmp_id := object_id(),
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
                forced_id => ForcedRef,
                references => list_term_to_binary(dmt_object_reference:refless_object_references(NewObject)),
                data => NewObject
            }};
        {error, Error} ->
            {error, Error}
    end.

update_object(
    #domain_conf_v2_UpdateOp{
        targeted_ref = {_, ID} = TargetedRef,
        new_object = NewObject
    },
    ExistingUpdate
) ->
    maybe
        {ok, Type} ?= get_checked_type(TargetedRef, NewObject),
        ok ?= check_domain_object_refs(TargetedRef, NewObject),
        {ok, ExistingUpdate#{
            id => ID,
            type => Type,
            %%          NOTE this will just provide all the refs that already exist,
            %%          it doesn't give us diff, but maybe it's not needed
            references => dmt_object_reference:domain_object_references(NewObject),
            data => NewObject
        }}
    end.

remove_object(OG) ->
    OG#{
        referenced_by => [],
        is_active => false
    }.

just_object(
    ID,
    Version,
    ReferencesTo,
    ReferencedBy,
    Data,
    CreatedAt,
    IsActive
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
        is_active => IsActive
    }.

get_checked_type(undefined, {Type, _}) ->
    {ok, Type};
get_checked_type({Type, _}, {Type, _}) ->
    {ok, Type};
get_checked_type(Ref, Object) ->
    {error, {type_mismatch, Ref, Object}}.

check_domain_object_refs({Type, Ref}, {Type, {_Object, Ref, _Data}}) ->
    ok;
check_domain_object_refs(Ref, Object) ->
    {error, {reference_mismatch, Ref, Object}}.

list_term_to_binary(Terms) ->
    lists:map(fun(Term) -> term_to_binary(Term) end, Terms).
