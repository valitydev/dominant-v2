-module(dmt_object_type).

-include_lib("damsel/include/dmsl_domain_thrift.hrl").

%% API
-export([get_refless_object_type/1]).
-export([get_ref_type/1]).

-export_type([refless_object_type/0, supported_refless_object/0, supported_object_ref/0]).

-type refless_object_type() :: category | currency.

%% NOTE: only the object/reference kinds enumerated below are classifiable at
%% the moment — every other domain structure falls through to `not_impl`. These
%% intermediate types name the currently-supported set explicitly; extend the
%% unions as support for new object types is added.
-type supported_refless_object() :: dmsl_domain_thrift:'Category'() | dmsl_domain_thrift:'Currency'().
-type supported_object_ref() :: dmsl_domain_thrift:'CategoryRef'() | dmsl_domain_thrift:'CurrencyRef'().

-spec get_refless_object_type(supported_refless_object() | term()) ->
    refless_object_type() | no_return().
get_refless_object_type(#domain_Category{}) ->
    category;
get_refless_object_type(#domain_Currency{}) ->
    currency;
get_refless_object_type(_) ->
    error(not_impl).

-spec get_ref_type(supported_object_ref() | undefined | term()) ->
    refless_object_type() | undefined | no_return().
get_ref_type(#domain_CurrencyRef{}) ->
    currency;
get_ref_type(#domain_CategoryRef{}) ->
    category;
get_ref_type(undefined) ->
    undefined;
get_ref_type(_) ->
    error(not_impl).
