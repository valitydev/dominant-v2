-- migrations/1786536402-add_external_party_routes.sql
-- :up
-- Up migration

INSERT INTO entity_type (name, has_sequence) VALUES
('external_party_routes', FALSE);

-- :down
-- Down migration

DELETE FROM entity_type WHERE name = 'external_party_routes';
