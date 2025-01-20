CREATE TABLE actors_history_scd (
	actor_id TEXT NOT NULL,
	actor TEXT NOT NULL,
	quality_class QUALITY_CLASS NOT NULL,
	is_active BOOLEAN NOT NULL DEFAULT FALSE,
	start_year INTEGER NOT NULL,
	end_year INTEGER NOT NULL,
	current_year INTEGER NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (actor_id, start_year, end_year, current_year),
	CONSTRAINT chk_valid_years CHECK(
		start_year <= end_year
		AND end_year <= current_year
		AND start_year >= 1970
	),
	CONSTRAINT chk_current_year CHECK(
		current_year >= 1970
	)
) PARTITION BY RANGE (current_year);

CREATE TYPE ACTORS_HISTORY_SCD_TYPE AS(
	quality_class QUALITY_CLASS,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER
);

CREATE INDEX idx_actors_history_scd_cy_ey_covering ON actors_history_scd(
    current_year, 
    end_year,
    actor_id,
    quality_class,
    is_active,
    start_year
);

CREATE INDEX idx_actors_history_scd_active ON actors_history_scd(actor_id, current_year)
WHERE is_active = TRUE;

CREATE INDEX idx_actors_history_scd_temporal ON actors_history_scd(start_year, end_year)
INCLUDE (actor_id, actor, quality_class, is_active);

CREATE TRIGGER trg_actors_history_scd_updated_at
    BEFORE UPDATE ON actors_history_scd
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE OR REPLACE FUNCTION validate_actors_history_scd() 
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.start_year > NEW.end_year THEN
        RAISE EXCEPTION 'Invalid temporal range: start_year (%) must be <= end_year (%)', 
            NEW.start_year, NEW.end_year;
    END IF;
    
    IF NEW.end_year > NEW.current_year THEN
        RAISE EXCEPTION 'Invalid temporal range: end_year (%) must be <= current_year (%)', 
            NEW.end_year, NEW.current_year;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_actors_history_scd_validate
    BEFORE INSERT OR UPDATE ON actors_history_scd
    FOR EACH ROW
    EXECUTE FUNCTION validate_actors_history_scd();

CREATE VIEW v_actors_current AS
SELECT 
    actor_id,
    actor,
    quality_class,
    is_active,
	start_year,
	end_year,
	current_year
FROM actors_history_scd
WHERE current_year = (SELECT MAX(current_year) FROM actors_history_scd);

CREATE VIEW v_actors_changes AS
SELECT 
    actor_id,
	actor,
    COUNT(*) AS version_count,
    MIN(start_year) AS first_appearance,
    MAX(end_year) AS last_update,
	COUNT(DISTINCT quality_class) AS quality_class_changes
FROM actors_history_scd
GROUP BY actor_id, actor;