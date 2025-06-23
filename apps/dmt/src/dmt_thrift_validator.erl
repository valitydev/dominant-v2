-module(dmt_thrift_validator).

-export([
    validate_domain_object/1,
    validate_reference/1,
    filter_search_results/1
]).

-include_lib("damsel/include/dmsl_domain_thrift.hrl").

%% @doc Validate a domain object using thrift strict validation
-spec validate_domain_object(dmsl_domain_thrift:'DomainObject'()) ->
    ok | {error, {invalid, [atom()], term()}}.
validate_domain_object(DomainObject) ->
    Type = dmsl_domain_thrift:struct_info('DomainObject'),
    thrift_strict_binary_codec:validate({Type, DomainObject}).

%% @doc Validate a reference
-spec validate_reference(dmsl_domain_thrift:'Reference'()) ->
    ok | {error, {invalid, [atom()], term()}}.
validate_reference(PayoutMethodRef) ->
    Type = dmsl_domain_thrift:struct_info('Reference'),
    thrift_strict_binary_codec:validate({Type, PayoutMethodRef}).

%% @doc Filter out invalid results from a list, keeping only valid ones
-spec filter_valid_results([term()], Type) -> [term()] when Type :: term().
filter_valid_results(Items, Type) ->
    lists:filter(
        fun(Item) ->
            case thrift_strict_binary_codec:validate({Type, Item}) of
                ok -> true;
                {error, _} -> false
            end
        end,
        Items
    ).

%% @doc Validate search results and filter out invalid ones
-spec filter_search_results([term()]) -> [term()].
filter_search_results(Results) ->
    Type = dmsl_domain_thrift:struct_info('DomainObject'),
    filter_valid_results(Results, Type).
