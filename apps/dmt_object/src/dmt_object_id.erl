-module(dmt_object_id).

-include_lib("damsel/include/dmsl_domain_thrift.hrl").

%% API
-export([get_numerical_object_id/2]).
-export([get_uuid_object_id/2]).

-type numerical_object_type() ::
    category
    | business_schedule
    | calendar
    | bank
    | term_set_hierarchy
    | payment_institution
    | provider
    | terminal
    | inspector
    | system_account_set
    | external_account_set
    | proxy
    | cash_register_provider
    | routing_rules
    | bank_card_category
    | criterion
    | document_type.

-type uuid_object_type() ::
    party_config
    | shop_config
    | wallet_config
    | partner.

-type numerical_ref() ::
    dmsl_domain_thrift:'CategoryRef'()
    | dmsl_domain_thrift:'BusinessScheduleRef'()
    | dmsl_domain_thrift:'CalendarRef'()
    | dmsl_domain_thrift:'BankRef'()
    | dmsl_domain_thrift:'TermSetHierarchyRef'()
    | dmsl_domain_thrift:'PaymentInstitutionRef'()
    | dmsl_domain_thrift:'ProviderRef'()
    | dmsl_domain_thrift:'TerminalRef'()
    | dmsl_domain_thrift:'InspectorRef'()
    | dmsl_domain_thrift:'SystemAccountSetRef'()
    | dmsl_domain_thrift:'ExternalAccountSetRef'()
    | dmsl_domain_thrift:'ProxyRef'()
    | dmsl_domain_thrift:'CashRegisterProviderRef'()
    | dmsl_domain_thrift:'RoutingRulesetRef'()
    | dmsl_domain_thrift:'BankCardCategoryRef'()
    | dmsl_domain_thrift:'CriterionRef'()
    | dmsl_domain_thrift:'DocumentTypeRef'().

-type uuid_ref() ::
    dmsl_domain_thrift:'PartyConfigRef'()
    | dmsl_domain_thrift:'ShopConfigRef'()
    | dmsl_domain_thrift:'WalletConfigRef'()
    | dmsl_domain_thrift:'PartnerRef'().

-export_type([numerical_object_type/0, uuid_object_type/0, numerical_ref/0, uuid_ref/0]).

-spec get_numerical_object_id(numerical_object_type() | atom(), dmsl_domain_thrift:'ObjectID'()) ->
    numerical_ref() | no_return().
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

-spec get_uuid_object_id(uuid_object_type() | atom(), dmsl_base_thrift:'ID'()) ->
    uuid_ref() | no_return().
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
