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


CREATE UNIQUE INDEX idx_places_source_source_id ON geo_places (source, source_id);
CREATE INDEX idx_places_source_source_path ON geo_places (source, source_path);


------------------------------

CREATE INDEX CONCURRENTLY idx_places_name_trgm ON geo_places USING gin (name gin_trgm_ops);



select name, type, similarity(name, 'mexico') similarity
from geo_places
where
  name % 'mexico'
  and similarity > 0.3
order by
	similarity desc,
	type
limit 50;





----------------------------------------------------------------------------------------------------
-- The original generated query and then modified a bunch. It's kind of overkill


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


-- Add index for text search
CREATE INDEX idx_places_search ON geo_places USING GIN(search_vector);

-- For trigram searches on name
CREATE INDEX CONCURRENTLY idx_places_name_trgm ON geo_places USING gin (name gin_trgm_ops);

-- For alternative names array
CREATE INDEX CONCURRENTLY idx_places_alt_names_gin ON geo_places USING gin (alternative_names);
CREATE INDEX CONCURRENTLY idx_places_alt_names_trgm ON geo_places USING gin (alternative_names gin_trgm_ops);



CREATE OR REPLACE FUNCTION find_place(search_term TEXT) RETURNS TABLE (
    source PlaceSourceType,
    source_path VARCHAR(255),
    source_id INTEGER,
    id INTEGER,
    name VARCHAR(255),
    type PlaceType,
    geom GEOMETRY,
    properties JSONB,
    similarity REAL
) AS $$
BEGIN
    RETURN QUERY
    WITH ranked_results AS (
        -- Direct name matches
        SELECT
          gp.*,
          1.0 as similarity,
          1 as source_priority  -- Highest priority for exact matches
        FROM geo_places gp
        WHERE gp.name ILIKE search_term

        UNION ALL

        -- Trigram similarity on name
        SELECT
            gp.*,
            similarity(gp.name, search_term) as similarity,
            2 as source_priority  -- Second priority for name similarities
        FROM geo_places gp
        WHERE gp.name % search_term
        AND gp.name NOT ILIKE search_term
        AND similarity(gp.name, search_term) > 0.3

        UNION ALL

        -- Commenting out alternative names for now since it's slow and we don't have any data in it
        -- -- Alternative names search
        -- (SELECT
        --     gp.*,
        --     similarity(unnest(alternative_names), search_term) as similarity
        -- FROM geo_places gp
        -- WHERE EXISTS (
        --     SELECT 1
        --     FROM unnest(alternative_names) alt_name
        --     WHERE similarity(alt_name, search_term) > 0.3
        -- )
        -- LIMIT 5)

        -- UNION ALL

        -- Full-text search
        SELECT
            gp.*,
            ts_rank(search_vector, query) as similarity,
            4 as source_priority  -- Lowest priority for full-text matches
        FROM geo_places gp,
        plainto_tsquery('english', search_term) query
        WHERE search_vector @@ query
        AND gp.name NOT ILIKE search_term
    )
   SELECT DISTINCT ON (id)  -- Add DISTINCT ON to eliminate duplicates
        source,
        source_path,
        source_id,
        id,
        name,
        type,
        geom,
        properties,
        similarity
    FROM all_matches
    ORDER BY
        id,              -- Required for DISTINCT ON
        source_priority, -- This ensures we keep the best match for each id
        similarity DESC,
        type
    LIMIT 10;
END;$$ LANGUAGE plpgsql;
