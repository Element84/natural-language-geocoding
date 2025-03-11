-- For fuzzy matching
CREATE EXTENSION pg_trgm;

-- For geographic functions (optional)
CREATE EXTENSION postgis;

-- For unaccent functionality (handles accents in names)
CREATE EXTENSION unaccent;

CREATE TYPE PlaceType AS ENUM (
    'continent',
    'ocean',
    'country',
    'macrocounty',
    'empire',
    'county',
    'region',
    'macroregion',
    'locality',
    'borough',
    'marinearea',
    'dependency',
    'disputed',
    'localadmin',
    'marketarea',
    'neighbourhood',
    'macrohood',
    'microhood',
    'postalregion',
);

create type PlaceSourceType as ENUM (
  'wof'
);


CREATE TABLE geo_places (
    id SERIAL PRIMARY KEY,

    -- Core fields
    name VARCHAR(255) NOT NULL,
    type PlaceType NOT NULL,
    geom GEOMETRY,
    -- Store common variations/spellings
    alternative_names TEXT[],
    properties JSONB,

    -- Identifying where this came from
    source PlaceSourceType NOT NULL,
    source_path VARCHAR(255) NOT NULL,
    source_id INTEGER NOT NULL,

    -- For full-text search
    search_vector TSVECTOR
);


-- Add index for text search
CREATE INDEX idx_places_search ON geo_places USING GIN(search_vector);

CREATE UNIQUE INDEX idx_places_source_source_id ON geo_places (source, source_id);


------------------------------

CREATE OR REPLACE FUNCTION update_place_search_vector() RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector =
        setweight(to_tsvector('english', NEW.name), 'A') ||
        setweight(to_tsvector('english', NEW.type::text), 'B') ||
        setweight(to_tsvector('english', array_to_string(COALESCE(NEW.alternative_names, '{}'::text[]), ' ')), 'C');
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER tsvector_update_trigger
  BEFORE INSERT OR UPDATE ON geo_places
  FOR EACH ROW EXECUTE PROCEDURE update_place_search_vector();

------------------------------



CREATE OR REPLACE FUNCTION find_place(search_term TEXT) RETURNS TABLE (
    id INTEGER,
    name VARCHAR(255),
    type PlaceType,
    geom GEOMETRY,
    properties JSONB,
    similarity REAL
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        gp.id,
        gp.name,
        gp.type,
        gp.geom,
        gp.properties,
        similarity(gp.name, search_term) AS similarity
    FROM
        geo_places gp
    WHERE
        gp.name % search_term
        OR search_term % ANY(gp.alternative_names)
        OR gp.search_vector @@ plainto_tsquery('english', search_term)
    ORDER BY
        similarity DESC,
        ts_rank(gp.search_vector, plainto_tsquery('english', search_term)) DESC,
        type
    LIMIT 10;
END;
$$ LANGUAGE plpgsql;
