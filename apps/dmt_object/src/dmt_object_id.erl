-module(dmt_object_id).

-include_lib("damsel/include/dmsl_domain_thrift.hrl").

%% API
-export([get_numerical_object_id/2]).
-export([get_uuid_object_id/2]).

-type numerical_id() :: integer().
-type uuid_id() :: binary().
-type type_tag() :: atom().

-export_type([numerical_id/0, uuid_id/0, type_tag/0]).

-spec get_numerical_object_id(type_tag(), numerical_id()) -> tuple() | no_return().
get_numerical_object_id(category, ID) ->
    #domain_CategoryRef{id = ID};
get_numerical_object_id(business_schedule, ID) ->
    #domain_BusinessScheduleRef{id = ID};
get_numerical_object_id(calendar, ID) ->
    #domain_CalendarRef{id = ID};
get_numerical_object_id(bank, ID) ->
    #domain_BankRef{id = ID};
get_numerical_object_id(term_set_hierarchy, ID) ->
    #domain_TermSetHierarchyRef{id = ID};
get_numerical_object_id(payment_institution, ID) ->
    #domain_PaymentInstitutionRef{id = ID};
get_numerical_object_id(provider, ID) ->
    #domain_ProviderRef{id = ID};
get_numerical_object_id(terminal, ID) ->
    #domain_TerminalRef{id = ID};
get_numerical_object_id(inspector, ID) ->
    #domain_InspectorRef{id = ID};
get_numerical_object_id(system_account_set, ID) ->
    #domain_SystemAccountSetRef{id = ID};
get_numerical_object_id(external_account_set, ID) ->
    #domain_ExternalAccountSetRef{id = ID};
get_numerical_object_id(proxy, ID) ->
    #domain_ProxyRef{id = ID};
get_numerical_object_id(cash_register_provider, ID) ->
    #domain_CashRegisterProviderRef{id = ID};
get_numerical_object_id(routing_rules, ID) ->
    #domain_RoutingRulesetRef{id = ID};
get_numerical_object_id(bank_card_category, ID) ->
    #domain_BankCardCategoryRef{id = ID};
get_numerical_object_id(criterion, ID) ->
    #domain_CriterionRef{id = ID};
get_numerical_object_id(document_type, ID) ->
    #domain_DocumentTypeRef{id = ID};
get_numerical_object_id(Type, _ID) ->
    throw({not_supported, Type}).

-spec get_uuid_object_id(type_tag(), uuid_id()) -> tuple() | no_return().
get_uuid_object_id(party_config, ID) ->
    #domain_PartyConfigRef{id = ID};
get_uuid_object_id(shop_config, ID) ->
    #domain_ShopConfigRef{id = ID};
get_uuid_object_id(wallet_config, ID) ->
    #domain_WalletConfigRef{id = ID};
get_uuid_object_id(partner, ID) ->
    #domain_PartnerRef{id = ID};
get_uuid_object_id(Type, _ID) ->
    throw({not_supported, Type}).
