-module(dmt_object_type).

-include_lib("damsel/include/dmsl_domain_thrift.hrl").

%% API
-export([get_refless_object_type/1]).
-export([get_ref_type/1]).

get_refless_object_type(#domain_Category{}) ->
    category;
get_refless_object_type(#domain_Currency{}) ->
    currency;
get_refless_object_type(_) ->
    error(not_impl).

get_ref_type(#domain_CurrencyRef{}) ->
    currency;
get_ref_type(#domain_CategoryRef{}) ->
    category;
get_ref_type(undefined) ->
    undefined;
get_ref_type(_) ->
    error(not_impl).
