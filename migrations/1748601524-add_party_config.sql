-- migrations/1748601524-add_party_config.sql
-- :up
-- Up migration

INSERT INTO entity_type (name, has_sequence) VALUES
('party_config', FALSE),
('shop_config', FALSE),
('wallet_config', FALSE);

-- :down
-- Down migration

DELETE FROM entity_type WHERE name = 'party_config';
DELETE FROM entity_type WHERE name = 'shop_config';
DELETE FROM entity_type WHERE name = 'wallet_config';
