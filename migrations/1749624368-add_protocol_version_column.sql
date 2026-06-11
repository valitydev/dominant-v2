-- migrations/1749624368-add_protocol_version_column.sql

ALTER TABLE version ADD COLUMN protocol_version TEXT NOT NULL DEFAULT '';
