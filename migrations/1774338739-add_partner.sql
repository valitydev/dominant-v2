-- migrations/1774338739-add_partner.sql
-- :up
-- Up migration

INSERT INTO entity_type (name, has_sequence) VALUES
('partner', FALSE);

-- :down
-- Down migration

DELETE FROM entity_type WHERE name = 'partner';
