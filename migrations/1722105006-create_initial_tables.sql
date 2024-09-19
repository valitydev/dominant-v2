-- migrations/1722105006-create_initial_tables.sql
-- :up
-- Up migration

CREATE TABLE op_user (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE TABLE global_version (
    version BIGSERIAL PRIMARY KEY,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE category (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE currency (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE business_schedule (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE calendar (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE payment_method (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE payout_method (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE bank (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE contract_template (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE term_set_hierarchy (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE payment_institution (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE provider (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE terminal (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE inspector (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE system_account_set (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE external_account_set (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE proxy (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE globals (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE cash_register_provider (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE routing_rules (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE bank_card_category (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE criterion (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE document_type (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE payment_service (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE payment_system (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE bank_card_token_service (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE mobile_op_user (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE crypto_currency (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE country (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE trade_bloc (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE identity_provider (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE TABLE limit_config (
    id JSONB PRIMARY KEY,
    global_version BIGINT NOT NULL REFERENCES global_version(version),
    references_to JSONB[] NOT NULL,
    referenced_by JSONB[] NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    created_by UUID REFERENCES op_user(id)
);

CREATE INDEX idx_category_global_version ON category(global_version);
CREATE INDEX idx_currency_global_version ON currency(global_version);
CREATE INDEX idx_business_schedule_global_version ON business_schedule(global_version);
CREATE INDEX idx_calendar_global_version ON calendar(global_version);
CREATE INDEX idx_payment_method_global_version ON payment_method(global_version);
CREATE INDEX idx_payout_method_global_version ON payout_method(global_version);
CREATE INDEX idx_bank_global_version ON bank(global_version);
CREATE INDEX idx_contract_template_global_version ON contract_template(global_version);
CREATE INDEX idx_term_set_hierarchy_global_version ON term_set_hierarchy(global_version);
CREATE INDEX idx_payment_institution_global_version ON payment_institution(global_version);
CREATE INDEX idx_provider_global_version ON provider(global_version);
CREATE INDEX idx_terminal_global_version ON terminal(global_version);
CREATE INDEX idx_inspector_global_version ON inspector(global_version);
CREATE INDEX idx_system_account_set_global_version ON system_account_set(global_version);
CREATE INDEX idx_external_account_set_global_version ON external_account_set(global_version);
CREATE INDEX idx_proxy_global_version ON proxy(global_version);
CREATE INDEX idx_globals_global_version ON globals(global_version);
CREATE INDEX idx_cash_register_provider_global_version ON cash_register_provider(global_version);
CREATE INDEX idx_routing_rules_global_version ON routing_rules(global_version);
CREATE INDEX idx_bank_card_category_global_version ON bank_card_category(global_version);
CREATE INDEX idx_criterion_global_version ON criterion(global_version);
CREATE INDEX idx_document_type_global_version ON document_type(global_version);
CREATE INDEX idx_payment_service_global_version ON payment_service(global_version);
CREATE INDEX idx_payment_system_global_version ON payment_system(global_version);
CREATE INDEX idx_bank_card_token_service_global_version ON bank_card_token_service(global_version);
CREATE INDEX idx_mobile_op_user_global_version ON mobile_op_user(global_version);
CREATE INDEX idx_crypto_currency_global_version ON crypto_currency(global_version);
CREATE INDEX idx_country_global_version ON country(global_version);
CREATE INDEX idx_trade_bloc_global_version ON trade_bloc(global_version);
CREATE INDEX idx_identity_provider_global_version ON identity_provider(global_version);
CREATE INDEX idx_limit_config_global_version ON limit_config(global_version);
-- :down
-- Down migration

DROP INDEX IF EXISTS idx_category_global_version;
DROP INDEX IF EXISTS idx_currency_global_version;
DROP INDEX IF EXISTS idx_business_schedule_global_version;
DROP INDEX IF EXISTS idx_calendar_global_version;
DROP INDEX IF EXISTS idx_payment_method_global_version;
DROP INDEX IF EXISTS idx_payout_method_global_version;
DROP INDEX IF EXISTS idx_bank_global_version;
DROP INDEX IF EXISTS idx_contract_template_global_version;
DROP INDEX IF EXISTS idx_term_set_hierarchy_global_version;
DROP INDEX IF EXISTS idx_payment_institution_global_version;
DROP INDEX IF EXISTS idx_provider_global_version;
DROP INDEX IF EXISTS idx_terminal_global_version;
DROP INDEX IF EXISTS idx_inspector_global_version;
DROP INDEX IF EXISTS idx_system_account_set_global_version;
DROP INDEX IF EXISTS idx_external_account_set_global_version;
DROP INDEX IF EXISTS idx_proxy_global_version;
DROP INDEX IF EXISTS idx_globals_global_version;
DROP INDEX IF EXISTS idx_cash_register_provider_global_version;
DROP INDEX IF EXISTS idx_routing_rules_global_version;
DROP INDEX IF EXISTS idx_bank_card_category_global_version;
DROP INDEX IF EXISTS idx_criterion_global_version;
DROP INDEX IF EXISTS idx_document_type_global_version;
DROP INDEX IF EXISTS idx_payment_service_global_version;
DROP INDEX IF EXISTS idx_payment_system_global_version;
DROP INDEX IF EXISTS idx_bank_card_token_service_global_version;
DROP INDEX IF EXISTS idx_mobile_operator_global_version;
DROP INDEX IF EXISTS idx_crypto_currency_global_version;
DROP INDEX IF EXISTS idx_country_global_version;
DROP INDEX IF EXISTS idx_trade_bloc_global_version;
DROP INDEX IF EXISTS idx_identity_provider_global_version;
DROP INDEX IF EXISTS idx_limit_config_global_version;

DROP TABLE IF EXISTS category;
DROP TABLE IF EXISTS currency;
DROP TABLE IF EXISTS business_schedule;
DROP TABLE IF EXISTS calendar;
DROP TABLE IF EXISTS payment_method;
DROP TABLE IF EXISTS payout_method;
DROP TABLE IF EXISTS bank;
DROP TABLE IF EXISTS contract_template;
DROP TABLE IF EXISTS term_set_hierarchy;
DROP TABLE IF EXISTS payment_institution;
DROP TABLE IF EXISTS provider;
DROP TABLE IF EXISTS terminal;
DROP TABLE IF EXISTS inspector;
DROP TABLE IF EXISTS system_account_set;
DROP TABLE IF EXISTS external_account_set;
DROP TABLE IF EXISTS proxy;
DROP TABLE IF EXISTS globals;
DROP TABLE IF EXISTS cash_register_provider;
DROP TABLE IF EXISTS routing_rules;
DROP TABLE IF EXISTS bank_card_category;
DROP TABLE IF EXISTS criterion;
DROP TABLE IF EXISTS document_type;
DROP TABLE IF EXISTS payment_service;
DROP TABLE IF EXISTS payment_system;
DROP TABLE IF EXISTS bank_card_token_service;
DROP TABLE IF EXISTS mobile_operator;
DROP TABLE IF EXISTS crypto_currency;
DROP TABLE IF EXISTS country;
DROP TABLE IF EXISTS trade_bloc;
DROP TABLE IF EXISTS identity_provider;
DROP TABLE IF EXISTS limit_config;

DROP TABLE IF EXISTS global_version;

DROP TABLE IF EXISTS op_user;
