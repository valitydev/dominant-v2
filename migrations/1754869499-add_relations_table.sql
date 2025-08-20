-- migrations/1754869499-add_relations_table.sql
-- :up
-- Up migration

ALTER TABLE entity ALTER COLUMN references_to DROP NOT NULL;
ALTER TABLE entity ALTER COLUMN referenced_by DROP NOT NULL;
ALTER TABLE entity DROP COLUMN references_to;
ALTER TABLE entity DROP COLUMN referenced_by;

CREATE TABLE IF NOT EXISTS entity_relation (
    source_entity_id TEXT NOT NULL,
    target_entity_id TEXT NOT NULL,
    version BIGINT NOT NULL REFERENCES version(version),
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    
    PRIMARY KEY (source_entity_id, target_entity_id, version)
);

CREATE OR REPLACE FUNCTION validate_entity_exists(entity_id TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    RETURN EXISTS(SELECT 1 FROM entity WHERE id = entity_id LIMIT 1);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION validate_entity_relation()
RETURNS TRIGGER AS $$
BEGIN
    IF NOT validate_entity_exists(NEW.source_entity_id) THEN
        RAISE EXCEPTION 'ENTITY_NOT_EXISTS|SOURCE|%', NEW.source_entity_id;
    END IF;
    
    IF NOT validate_entity_exists(NEW.target_entity_id) THEN
        RAISE EXCEPTION 'ENTITY_NOT_EXISTS|TARGET|%', NEW.target_entity_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_validate_entity_relation
    BEFORE INSERT OR UPDATE ON entity_relation
    FOR EACH ROW
    EXECUTE FUNCTION validate_entity_relation();

CREATE INDEX idx_entity_relation_source ON entity_relation(source_entity_id);
CREATE INDEX idx_entity_relation_target ON entity_relation(target_entity_id);
CREATE INDEX idx_entity_relation_version ON entity_relation(version);
CREATE INDEX idx_entity_relation_active ON entity_relation(source_entity_id, target_entity_id, is_active);

-- :down
-- Down migration

DROP INDEX IF EXISTS idx_entity_relation_active;
DROP INDEX IF EXISTS idx_entity_relation_version;
DROP INDEX IF EXISTS idx_entity_relation_target;
DROP INDEX IF EXISTS idx_entity_relation_source;

DROP TRIGGER IF EXISTS trigger_validate_entity_relation ON entity_relation;
DROP FUNCTION IF EXISTS validate_entity_relation();
DROP FUNCTION IF EXISTS validate_entity_exists(TEXT);

DROP TABLE IF EXISTS entity_relation;

ALTER TABLE entity ADD COLUMN references_to TEXT[] NOT NULL DEFAULT '{}';
ALTER TABLE entity ADD COLUMN referenced_by TEXT[] NOT NULL DEFAULT '{}'; 
