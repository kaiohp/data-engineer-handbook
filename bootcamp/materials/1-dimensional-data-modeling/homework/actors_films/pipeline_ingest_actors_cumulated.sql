DO $$
BEGIN
FOR ingest_year IN 1970..2021 LOOP
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
			ARRAY_AGG(ROW(filmid, film, year, votes, rating)::FILMS) AS films,
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
		coalesce(cy.actor_id, py.actor_id),
		coalesce(cy.actor, py.actor),
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
		cy.year IS NOT NULL as is_active,
		COALESCE(cy.year, py.current_year + 1) AS current_year
	FROM
		current_year cy
	FULL OUTER JOIN
		previous_year py
	ON cy.actor_id = py.actor_id;
END LOOP;
END $$;