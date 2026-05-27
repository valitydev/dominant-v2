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
-export_type([enriched_object/0]).
-export_type([object_type/0]).
-export_type([object_id/0]).

-type object_type() :: atom().
-type object_id() :: dmsl_domain_thrift:'Reference'().
-type timestamp() :: dmsl_base_thrift:'Timestamp'().
-type version() :: dmsl_domain_conf_v2_thrift:'Version'().

-type insertable_object() :: #{
    type := object_type(),
    tmp_id := binary(),
    forced_id := dmsl_domain_thrift:'Reference'() | undefined,
    data := dmsl_domain_thrift:'ReflessDomainObject'()
}.

-type object_changes() :: #{
    id := object_id(),
    type := object_type(),
    data := dmsl_domain_thrift:'DomainObject'(),
    referenced_by => [object_id()],
    is_active => boolean()
}.

-type object() :: #{
    id := object_id(),
    type := object_type(),
    version := version(),
    data := dmsl_domain_thrift:'DomainObject'(),
    created_at := timestamp(),
    is_active := boolean()
}.

%% `object()` enriched with the resolved author. Produced by
%% `dmt_repository:add_created_by_to_object/2`.
-type enriched_object() :: #{
    id := object_id(),
    type := object_type(),
    version := version(),
    data := dmsl_domain_thrift:'DomainObject'(),
    created_at := timestamp(),
    is_active := boolean(),
    created_by := dmsl_domain_conf_v2_thrift:'Author'() | undefined
}.

-spec new_object(dmsl_domain_conf_v2_thrift:'InsertOp'()) ->
    {ok, insertable_object()} | {error, {type_mismatch, dmsl_domain_thrift:'Reference'(), term()}}.
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

-spec update_object(dmsl_domain_thrift:'DomainObject'() | term(), object_changes()) ->
    {ok, object_changes()} | {error, {is_not_domain_object, term()}}.
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

-spec remove_object(object_changes()) -> object_changes().
remove_object(OG) ->
    OG#{
        referenced_by => [],
        is_active => false
    }.

-spec just_object(
    object_id(),
    object_type(),
    version(),
    dmsl_domain_thrift:'DomainObject'(),
    timestamp(),
    boolean()
) -> object().
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

-spec filter_out_inactive_objects([#{is_active := boolean(), _ => _}]) ->
    [#{is_active := boolean(), _ => _}].
filter_out_inactive_objects(Objects) ->
    lists:filter(
        fun(Obj) ->
            maps:get(is_active, Obj)
        end,
        Objects
    ).

-spec get_checked_type(dmsl_domain_thrift:'Reference'() | undefined, dmsl_domain_thrift:'ReflessDomainObject'()) ->
    {ok, object_type()} | {error, {type_mismatch, dmsl_domain_thrift:'Reference'(), term()}}.
get_checked_type(undefined, {Type, _}) ->
    {ok, Type};
get_checked_type({Type, _}, {Type, _}) ->
    {ok, Type};
get_checked_type(Ref, Object) ->
    {error, {type_mismatch, Ref, Object}}.

-spec get_object_type(dmsl_domain_thrift:'DomainObject'() | term()) ->
    {ok, object_type()} | {error, {is_not_domain_object, term()}}.
get_object_type({Type, {_Object, _Ref, _Data}}) ->
    {ok, Type};
get_object_type(Obj) ->
    {error, {is_not_domain_object, Obj}}.

-spec get_object_ref(dmsl_domain_thrift:'DomainObject'() | term()) ->
    {ok, object_id()} | {error, {is_not_domain_object, term()}}.
get_object_ref({Type, {_Object, ID, _Data}}) ->
    {ok, {Type, ID}};
get_object_ref(Obj) ->
    {error, {is_not_domain_object, Obj}}.
