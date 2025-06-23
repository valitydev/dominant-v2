-- migrations/1749624368-add_protocol_version_column.sql
-- :up
-- Up migration

ALTER TABLE version ADD COLUMN protocol_version TEXT NOT NULL DEFAULT '';

-- :down
-- Down migration

ALTER TABLE version DROP COLUMN protocol_version;
