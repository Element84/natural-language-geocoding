-- For fuzzy matching
CREATE EXTENSION pg_trgm;

-- For geographic functions (optional)
CREATE EXTENSION postgis;

-- For unaccent functionality (handles accents in names)
CREATE EXTENSION unaccent;

CREATE TABLE geo_places (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) NOT NULL, -- ocean, country, river, etc.
    geom GEOMETRY, -- If you want to store spatial data
    search_vector TSVECTOR, -- For full-text search
    alternative_names TEXT[] -- Store common variations/spellings
);

-- Add index for text search
CREATE INDEX idx_places_search ON geo_places USING GIN(search_vector);



CREATE OR REPLACE FUNCTION update_place_search_vector() RETURNS TRIGGER AS $$
BEGIN
    NEW.search_vector =
        setweight(to_tsvector('english', NEW.name), 'A') ||
        setweight(to_tsvector('english', COALESCE(NEW.type, '')), 'B') ||
        setweight(to_tsvector('english', array_to_string(COALESCE(NEW.alternative_names, '{}'::text[]), ' ')), 'C');
    RETURN NEW;
END
$$ LANGUAGE plpgsql;

CREATE TRIGGER tsvector_update_trigger
BEFORE INSERT OR UPDATE ON geo_places
FOR EACH ROW EXECUTE PROCEDURE update_place_search_vector();

CREATE OR REPLACE FUNCTION find_place(search_term TEXT) RETURNS TABLE (
    id INTEGER,
    name VARCHAR(255),
    type VARCHAR(50),
    geom GEOMETRY,
    similarity FLOAT
) AS $$
BEGIN
    RETURN QUERY
    SELECT
        gp.id,
        gp.name,
        gp.type,
        gp.geom,
        similarity(gp.name, search_term) AS similarity
    FROM
        geo_places gp
    WHERE
        gp.name % search_term
        OR search_term % ANY(gp.alternative_names)
        OR gp.search_vector @@ plainto_tsquery('english', search_term)
    ORDER BY
        similarity DESC,
        ts_rank(gp.search_vector, plainto_tsquery('english', search_term)) DESC
    LIMIT 10;
END;
$$ LANGUAGE plpgsql;
