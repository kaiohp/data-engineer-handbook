CREATE TYPE FILMS AS(
	film_id TEXT,
	film TEXT,
	year INTEGER,
	votes INTEGER,
	rating REAL
);

CREATE TYPE QUALITY_CLASS AS ENUM('bad', 'average', 'good', 'star');

CREATE TABLE actors (
	actor_id TEXT NOT NULL,
	actor TEXT NOT NULL,
	films FILMS[] NOT NULL CHECK (CARDINALITY(films) > 0),
	quality_class QUALITY_CLASS NOT NULL,
	average_rating REAL NOT NULL,
	is_active BOOLEAN NOT NULL DEFAULT FALSE,
	current_year INTEGER NOT NULL,
	created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
	total_films INTEGER GENERATED ALWAYS AS (CARDINALITY(films)) STORED,
	PRIMARY KEY (actor_id, current_year),
	CONSTRAINT chk_current_year CHECK (
		current_year >= 1970
		AND current_year <= EXTRACT(YEAR FROM CURRENT_TIMESTAMP)
	),
	CONSTRAINT chk_films_array_size CHECK (
		CARDINALITY(films) <= 350
	)
) PARTITION BY RANGE (current_year);

CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS trigger AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_actors_updated_at
    BEFORE UPDATE ON actors
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at();

CREATE OR REPLACE FUNCTION validate_films_array()
RETURNS trigger AS $$
DECLARE
    film_record FILMS;
BEGIN
    FOREACH film_record IN ARRAY NEW.films
    LOOP
        IF film_record.year < 1970 OR film_record.year > NEW.current_year THEN
            RAISE EXCEPTION 'Invalid year (%) for film %', film_record.year, film_record.film;
        END IF;
        
        IF film_record.rating < 0 OR film_record.rating > 10 THEN
            RAISE EXCEPTION 'Invalid rating (%) for film %', film_record.rating, film_record.film;
        END IF;
        
        IF film_record.votes < 0 THEN
            RAISE EXCEPTION 'Invalid votes count (%) for film %', film_record.votes, film_record.film;
        END IF;
    END LOOP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_actors_validate_films
    BEFORE INSERT OR UPDATE ON actors
    FOR EACH ROW
    EXECUTE FUNCTION validate_films_array();


CREATE INDEX idx_actors_temporal_status ON actors (
    current_year,
    quality_class,
	average_rating,
    is_active
) INCLUDE (
    actor_id,
    actor,
    total_films
);


CREATE VIEW v_actors_stats AS
SELECT 
    actor_id,
    actor,
    quality_class,
	average_rating,
    total_films,
    is_active,
    current_year,
    films[1].year AS first_film_year,
    films[CARDINALITY(films)].year AS last_film_year
FROM actors,
LATERAL unnest(films) AS f(film_id, film, year, votes, rating)
WHERE current_year = (SELECT MAX(current_year) FROM actors)
GROUP BY actor_id, actor, quality_class, is_active, current_year;