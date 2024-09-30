-module(dmt_v2_object_id).

-include_lib("damsel/include/dmsl_domain_thrift.hrl").

%% API
-export([get_numerical_object_id/2]).

get_numerical_object_id(Type, ID) ->
    {Type, get_numerical_object_id_(Type, ID)}.

get_numerical_object_id_(category, ID) ->
    #domain_CategoryRef{id = ID};
get_numerical_object_id_(business_schedule, ID) ->
    #domain_BusinessScheduleRef{id = ID};
get_numerical_object_id_(calendar, ID) ->
    #domain_CalendarRef{id = ID};
get_numerical_object_id_(bank, ID) ->
    #domain_BankRef{id = ID};
get_numerical_object_id_(contract_template, ID) ->
    #domain_ContractTemplateRef{id = ID};
get_numerical_object_id_(term_set_hierarchy, ID) ->
    #domain_TermSetHierarchyRef{id = ID};
get_numerical_object_id_(payment_institution, ID) ->
    #domain_PaymentInstitutionRef{id = ID};
get_numerical_object_id_(provider, ID) ->
    #domain_ProviderRef{id = ID};
get_numerical_object_id_(terminal, ID) ->
    #domain_TerminalRef{id = ID};
get_numerical_object_id_(inspector, ID) ->
    #domain_InspectorRef{id = ID};
get_numerical_object_id_(system_account_set, ID) ->
    #domain_SystemAccountSetRef{id = ID};
get_numerical_object_id_(external_account_set, ID) ->
    #domain_ExternalAccountSetRef{id = ID};
get_numerical_object_id_(proxy, ID) ->
    #domain_ProxyRef{id = ID};
get_numerical_object_id_(cash_register_provider, ID) ->
    #domain_CashRegisterProviderRef{id = ID};
get_numerical_object_id_(routing_rules, ID) ->
    #domain_RoutingRulesetRef{id = ID};
get_numerical_object_id_(bank_card_category, ID) ->
    #domain_BankCardCategoryRef{id = ID};
get_numerical_object_id_(criterion, ID) ->
    #domain_CriterionRef{id = ID};
get_numerical_object_id_(document_type, ID) ->
    #domain_DocumentTypeRef{id = ID};
get_numerical_object_id_(Type, _ID) ->
    throw({not_supported, Type}).
