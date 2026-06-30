UPDATE entity
SET search_vector = tmp.search_vector
FROM (SELECT id, version, search_vector FROM  tmp_entity_search_vector) AS tmp
WHERE entity.id = tmp.id AND entity.version = tmp.version;
