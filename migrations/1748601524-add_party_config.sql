-- migrations/1748601524-add_party_config.sql

INSERT INTO entity_type (name, has_sequence) VALUES
('party_config', FALSE),
('shop_config', FALSE),
('wallet_config', FALSE);
