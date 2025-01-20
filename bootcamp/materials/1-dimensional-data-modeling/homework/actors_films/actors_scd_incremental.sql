DO $$
DECLARE 
    current_year_param INT := 2021;
    previous_year_param INT := current_year_param - 1;

BEGIN

EXECUTE format('CREATE TABLE actors_history_scd_%s PARTITION OF actors_history_scd
                       FOR VALUES FROM (%s) TO (%s)',
                      current_year_param,
                      current_year_param,
                      current_year_param + 1);

INSERT INTO actors_history_scd
WITH actors_unchanged_history AS (
	SELECT
		actor_id,
		actor,
		quality_class,
		is_active,
		start_year,
		end_year,
		current_year_param AS current_year
	FROM
		actors_history_scd
	WHERE
		current_year = previous_year_param
		AND end_year < previous_year_param
),
actors_previous_year AS (
	SELECT
		actor_id,
		actor,
		quality_class,
		is_active,
		start_year,
		end_year,
		current_year
	FROM
		actors_history_scd
	WHERE
		current_year = previous_year_param
		AND end_year = previous_year_param 
),
duplicate_records AS (
    SELECT 
        actor_id,
        current_year,
        COUNT(*) AS record_count
    FROM actors
    WHERE current_year = current_year_param
    GROUP BY actor_id, current_year
    HAVING COUNT(*) > 1
),
null_value_records AS (
    SELECT 
        actor_id,
        ARRAY_AGG(current_year) AS years_with_nulls,
        COUNT(*) AS null_count
    FROM actors 
    WHERE current_year = current_year_param
    AND (
        actor_id IS NULL 
        OR quality_class IS NULL 
        OR is_active IS NULL
        OR current_year IS NULL
    )
    GROUP BY actor_id
),
validation_check_summary AS (
    SELECT 
        (SELECT COUNT(*) FROM duplicate_records) as duplicate_count,
        (SELECT COUNT(*) FROM null_value_records) as null_count,
        CASE 
            WHEN EXISTS (SELECT 1 FROM duplicate_records) THEN 0
            WHEN EXISTS (SELECT 1 FROM null_value_records) THEN 0
            ELSE 1
        END as is_valid
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
	WHERE 
		current_year = current_year_param
		AND EXISTS (SELECT 1 FROM validation_check_summary WHERE is_valid = 1)
),
unchanged_current_records AS (
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
		actors_previous_year py
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
				ROW(py.quality_class, py.is_active, py.start_year, py.end_year)::ACTORS_HISTORY_SCD_TYPE,
				ROW(cy.quality_class, cy.is_active, cy.current_year, cy.current_year)::ACTORS_HISTORY_SCD_TYPE
			]
		)::ACTORS_HISTORY_SCD_TYPE).*,
		cy.current_year
	FROM
		actors_current_year as cy
	INNER JOIN 
		actors_previous_year py
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
		SELECT 1 FROM actors_previous_year py WHERE cy.actor_id = py.actor_id
	)
)
SELECT * FROM actors_unchanged_history
UNION ALL
SELECT * FROM unchanged_current_records
UNION ALL
SELECT * FROM changed_records
UNION ALL
SELECT * FROM new_records;
END $$;