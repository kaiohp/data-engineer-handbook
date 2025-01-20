DO $$
DECLARE
	start_year_for_backfill INT:= 1970;
    end_year_for_backfill INT := 2021;
BEGIN
FOR ingest_year IN start_year_for_backfill..end_year_for_backfill LOOP

EXECUTE format(
            'CREATE TABLE IF NOT EXISTS actors_y%s PARTITION OF actors FOR VALUES FROM (%s) TO (%s)',
            ingest_year, ingest_year, ingest_year + 1
        );

INSERT INTO actors
	WITH previous_year AS (
		SELECT
			*
		FROM
			actors
		WHERE 
			current_year = ingest_year - 1
	),
	current_year AS (
		SELECT
			actorid AS actor_id,
			actor,
			year,
			ARRAY_AGG(ROW(filmid, film, year, votes, rating)::FILMS ORDER BY year, rating) AS films,
			AVG(rating) AS average_rating
		FROM
			actor_films
		WHERE
			year = ingest_year
		GROUP BY
			actorid,
			actor,
			year
	)
	SELECT
		COALESCE(cy.actor_id, py.actor_id),
		COALESCE(cy.actor, py.actor),
		CASE
			WHEN py.films IS NULL
				THEN COALESCE(cy.films, ARRAY[]::FILMS[])
			WHEN cy.films IS NOT NULL
				THEN py.films || cy.films
			ELSE py.films
		END AS films,
		CASE
			WHEN cy.average_rating IS NOT NULL AND cy.average_rating > 8 THEN 'star'
			WHEN cy.average_rating IS NOT NULL AND cy.average_rating > 7 THEN 'good'
			WHEN cy.average_rating IS NOT NULL AND cy.average_rating > 6 THEN 'average'
			WHEN cy.average_rating IS NOT NULL AND cy.average_rating <= 6 THEN 'bad'
			ELSE
				py.quality_class
		END::QUALITY_CLASS AS quality_class,
		COALESCE(cy.average_rating, py.average_rating) AS average_rating,
		cy.year IS NOT NULL AS is_active,
		COALESCE(cy.year, py.current_year + 1) AS current_year
	FROM
		current_year cy
	FULL OUTER JOIN
		previous_year py
	ON cy.actor_id = py.actor_id;
END LOOP;
END $$;