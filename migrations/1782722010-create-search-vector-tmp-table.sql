CREATE TABLE tmp_entity_search_vector (
    id TEXT NOT NULL,
    version BIGINT NOT NULL REFERENCES version(version),
    search_vector tsvector,
    PRIMARY KEY (id, version)
);
