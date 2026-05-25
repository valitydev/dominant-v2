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
-export_type([object_type/0]).
-export_type([object_id/0]).
-export_type([timestamp/0]).
-export_type([domain_object/0]).
-export_type([refless_domain_object/0]).

-type object_type() :: atom().
-type object_id() :: term().
-type timestamp() :: binary() | string().

-type domain_object() :: dmsl_domain_thrift:'DomainObject'().
-type refless_domain_object() :: dmsl_domain_thrift:'ReflessDomainObject'().

-type insertable_object() :: #{
    tmp_id := binary(),
    type := object_type(),
    forced_id := term() | undefined,
    data := refless_domain_object()
}.

-type object_changes() :: #{
    id := {object_type(), object_id()},
    type := object_type(),
    %% Carries a domain object value; the shape is enforced by the producer
    %% (`update_object/2` / `commit_operation/2`) and not by this type.
    data := term(),
    referenced_by => [term()],
    is_active => boolean()
}.

-type object() :: #{
    id := term(),
    type := object_type(),
    version := number() | binary(),
    data := term(),
    created_at := binary() | list(),
    is_active := boolean(),
    atom() => term()
}.

-spec new_object(dmsl_domain_conf_v2_thrift:'InsertOp'()) ->
    {ok, insertable_object()} | {error, {type_mismatch, term(), term()}}.
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

-spec update_object(domain_object() | term(), object_changes()) ->
    {ok, object_changes()} | {error, {is_not_domain_object, term()}}.
update_object({Type, {_Record, ID, _Data}} = Object, ExistingUpdate) when is_atom(Type) ->
    {ok, ExistingUpdate#{
        id => {Type, ID},
        type => Type,
        data => Object
    }};
update_object(Obj, _ExistingUpdate) ->
    {error, {is_not_domain_object, Obj}}.

-spec remove_object(object_changes()) -> object_changes().
remove_object(OG) ->
    OG#{
        referenced_by => [],
        is_active => false
    }.

-spec just_object(term(), term(), term(), term(), term(), term()) -> object().
just_object(
    ID,
    Type,
    Version,
    Data,
    CreatedAt,
    IsActive
) when
    is_atom(Type),
    is_integer(Version) orelse is_binary(Version),
    is_binary(CreatedAt) orelse is_list(CreatedAt),
    is_boolean(IsActive)
->
    #{
        id => ID,
        type => Type,
        version => Version,
        data => Data,
        created_at => CreatedAt,
        is_active => IsActive
    }.

-spec get_checked_type(term() | undefined, refless_domain_object()) ->
    {ok, object_type()} | {error, {type_mismatch, term(), term()}}.
get_checked_type(undefined, {Type, _}) ->
    {ok, Type};
get_checked_type({Type, _}, {Type, _}) ->
    {ok, Type};
get_checked_type(Ref, Object) ->
    {error, {type_mismatch, Ref, Object}}.

-spec filter_out_inactive_objects([object()]) -> [object()].
filter_out_inactive_objects(Objects) ->
    lists:filter(
        fun(Obj) ->
            maps:get(is_active, Obj)
        end,
        Objects
    ).
