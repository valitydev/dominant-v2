-- migrations/1745545876-add_dummy.sql
-- :up
-- Up migration

INSERT INTO entity_type (name, has_sequence) VALUES
('dummy', FALSE),
('dummy_link', FALSE);

-- :down
-- Down migration

DELETE FROM entity_type WHERE name = 'dummy';
DELETE FROM entity_type WHERE name = 'dummy_link';
