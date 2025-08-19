-module(dmt_object).

-feature(maybe_expr, enable).

-include_lib("damsel/include/dmsl_domain_conf_v2_thrift.hrl").

-export([new_object/1]).
-export([update_object/2]).
-export([remove_object/1]).
-export([just_object/6]).
-export([filter_out_inactive_objects/1]).

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
    version := string(),
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
                data => NewObject
            }};
        {error, Error} ->
            {error, Error}
    end.

update_object(
    Object,
    ExistingUpdate
) ->
    maybe
        {ok, Type} ?= get_object_type(Object),
        {ok, ID} ?= get_object_ref(Object),
        {ok, ExistingUpdate#{
            id => ID,
            type => Type,
            data => Object
        }}
    end.

remove_object(OG) ->
    OG#{
        referenced_by => [],
        is_active => false
    }.

just_object(
    ID,
    Type,
    Version,
    Data,
    CreatedAt,
    IsActive
) ->
    #{
        id => ID,
        type => Type,
        version => Version,
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

get_object_type({Type, {_Object, _Ref, _Data}}) ->
    {ok, Type};
get_object_type(Obj) ->
    {error, {is_not_domain_object, Obj}}.

get_object_ref({Type, {_Object, ID, _Data}}) ->
    {ok, {Type, ID}};
get_object_ref(Obj) ->
    {error, {is_not_domain_object, Obj}}.

filter_out_inactive_objects(Objects) ->
    lists:filter(
        fun(Obj) ->
            maps:get(is_active, Obj)
        end,
        Objects
    ).
