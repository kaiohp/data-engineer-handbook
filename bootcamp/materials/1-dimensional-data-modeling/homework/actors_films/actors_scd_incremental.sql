INSERT INTO actors_history_scd
WITH actors_previous_year_scd AS (
	SELECT
		*
	FROM
		actors_history_scd
	WHERE
		current_year = 2020
		AND end_year = 2020 
),
actors_history_scd AS (
	SELECT
		actor_id,
		actor,
		quality_class,
		is_active,
		start_year,
		end_year,
		2021 AS current_year
	FROM
		actors_history_scd
	WHERE
		current_year = 2020
		AND end_year < 2020
),
actors_current_year AS (
	SELECT
		actor_id,
		actor,
		quality_class,
		is_active,
		current_year
	FROM
		actors
	WHERE current_year = 2021
),
unchanged_records AS (
	SELECT
		cy.actor_id,
		cy.actor,
		cy.quality_class,
		cy.is_active,
		py.start_year,
		cy.current_year as end_year,
		cy.current_year
	FROM
		actors_current_year as cy
	INNER JOIN 
		actors_previous_year_scd py
	ON
		cy.actor_id = py.actor_id
	AND cy.quality_class = py.quality_class
	AND cy.is_active = py.is_active
),
changed_records AS(
	SELECT
		cy.actor_id,
		cy.actor,
		(UNNEST(
			ARRAY[
				ROW(
					py.quality_class,
					py.is_active,
					py.start_year,
					py.end_year
				)::ACTORS_HISTORY_SCD_TYPE,
				ROW(
					cy.quality_class,
					cy.is_active,
					cy.current_year,
					cy.current_year
				)::ACTORS_HISTORY_SCD_TYPE
			]
		)::ACTORS_HISTORY_SCD_TYPE).*,
		cy.current_year
	FROM
		actors_current_year as cy
	INNER JOIN 
		actors_previous_year_scd py
	ON
		cy.actor_id = py.actor_id
	AND ( 
		cy.quality_class <> py.quality_class
		OR cy.is_active <> py.is_active 
		)
),
new_records AS (
	SELECT
		cy.actor_id,
		cy.actor,
		cy.quality_class,
		cy.is_active,
		cy.current_year AS start_year,
		cy.current_year AS end_year,
		cy.current_year
	FROM
		actors_current_year as cy
	WHERE NOT EXISTS(
		SELECT 1 FROM actors_previous_year_scd py WHERE cy.actor_id = py.actor_id
	)
)
SELECT * FROM actors_history_scd
UNION ALL
SELECT * FROM unchanged_records
UNION ALL
SELECT * FROM changed_records
UNION ALL
SELECT * FROM new_records;