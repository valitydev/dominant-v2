UPDATE entity AS entity
SET entity.search_vector = buffer.search_vector
FROM (SELECT id, version, search_vector FROM  tmp_entity_search_vector) AS buffer
WHERE entity.id = buffer.id AND entity.version = buffer.version;
