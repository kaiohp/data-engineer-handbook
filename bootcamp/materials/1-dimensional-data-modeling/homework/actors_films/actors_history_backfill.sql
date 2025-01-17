INSERT INTO actors_history_scd
WITH actors_with_previous_values AS(
	SELECT 
		actor_id,
		actor,
		current_year,
		quality_class,
		is_active,
		LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_quality_class,
		LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) AS previous_is_active
	FROM actors
	WHERE current_year <= 2020
),
actors_with_change_indicator AS (
	SELECT
		*,
		CASE
			WHEN quality_class <> previous_quality_class THEN 1
			WHEN is_active <> previous_is_active THEN 1
			ELSE 0
		END AS change_indicator
	FROM
		actors_with_previous_values
),
actors_with_streak AS(
SELECT
	*,
	SUM(change_indicator) OVER (PARTITION BY actor_id ORDER BY current_year) as streak
FROM 
	actors_with_change_indicator
)
SELECT
	actor_id,
	actor,
	quality_class,
	is_active,
	MIN(current_year) AS start_year,
	MAX(current_year) AS end_year,
	2020 AS current_year
FROM
	actors_with_streak
GROUP BY 
	actor_id,
	actor,
	quality_class,
	is_active,
	streak
ORDER BY
	actor_id,
	streak;