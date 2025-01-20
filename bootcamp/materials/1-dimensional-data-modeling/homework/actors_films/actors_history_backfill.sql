DO $$
DECLARE 
    current_year_param INT := 2020;

BEGIN

EXECUTE format('CREATE TABLE actors_history_scd_%s PARTITION OF actors_history_scd
                       FOR VALUES FROM (%s) TO (%s)',
                      current_year_param,
                      current_year_param,
                      current_year_param + 1);

INSERT INTO actors_history_scd
WITH duplicate_records AS (
    SELECT 
        actor_id,
        current_year,
        COUNT(*) as record_count
    FROM actors
    WHERE current_year <= current_year_param
    GROUP BY actor_id, current_year
    HAVING COUNT(*) > 1
),
null_value_records AS (
    SELECT 
        actor_id,
        ARRAY_AGG(current_year) as years_with_nulls,
        COUNT(*) as null_count
    FROM actors 
    WHERE current_year <= current_year_param
    AND (
        actor_id IS NULL 
        OR quality_class IS NULL 
        OR is_active IS NULL
        OR current_year IS NULL
    )
    GROUP BY actor_id
),
invalid_year_records AS (
    SELECT 
        actor_id,
        current_year,
        COUNT(*) as invalid_count
    FROM actors 
    WHERE current_year <= current_year_param
    AND current_year < 1970  -- minimum valid year
    GROUP BY actor_id, current_year
),
validation_check_summary AS (
    SELECT 
        (SELECT COUNT(*) FROM duplicate_records) as duplicate_count,
        (SELECT COUNT(*) FROM null_value_records) as null_count,
        (SELECT COUNT(*) FROM invalid_year_records) as invalid_year_count,
        CASE 
            WHEN EXISTS (SELECT 1 FROM duplicate_records) THEN 0
            WHEN EXISTS (SELECT 1 FROM null_value_records) THEN 0
            WHEN EXISTS (SELECT 1 FROM invalid_year_records) THEN 0
            ELSE 1
        END as is_valid
),
with_previous_values AS(
	SELECT 
		actor_id,
		actor,
		current_year,
		quality_class,
		is_active,
		COALESCE(LAG(quality_class, 1) OVER w, quality_class) AS previous_quality_class,
		COALESCE(LAG(is_active, 1) OVER w, is_active) AS previous_is_active,
		ROW_NUMBER() OVER w as rn
	FROM actors
	WHERE 
		current_year <= current_year_param
		AND EXISTS (SELECT 1 FROM validation_check_summary WHERE is_valid = 1) 
	WINDOW W AS (PARTITION BY actor_id ORDER BY current_year)
),
with_change_indicator AS (
	SELECT
		*,
		CASE
			WHEN rn = 1 THEN 1
			WHEN quality_class IS DISTINCT FROM previous_quality_class 
				 OR is_active IS DISTINCT FROM previous_is_active THEN 1
			ELSE 0
		END AS change_indicator
	FROM
		with_previous_values
),
with_version AS(
SELECT
	actor_id,
	actor,
	quality_class,
	is_active,
	current_year,
	SUM(change_indicator) OVER (PARTITION BY actor_id ORDER BY current_year) as version_number
FROM 
	with_change_indicator
)
SELECT
	actor_id,
	actor,
	quality_class,
	is_active,
	MIN(current_year) AS start_year,
	MAX(current_year) AS end_year,
	current_year_param AS current_year
FROM
	with_version
GROUP BY 
	actor_id,
	actor,
	quality_class,
	is_active,
	version_number
HAVING MIN(current_year) <=  MAX(current_year)
ORDER BY
	actor_id,
	version_number;
END $$;