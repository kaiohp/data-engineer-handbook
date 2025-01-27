DO $$
DECLARE
	month_start_param DATE := '2023-01-01'::DATE;
	month_end_param DATE := (month_start_param + INTERVAL '1 month' - INTERVAL '1 day')::DATE;
	current_date_param DATE;
BEGIN
	FOR current_date_param IN SELECT * FROM GENERATE_SERIES(month_start_param, month_end_param, '1 day'::INTERVAL) LOOP
		INSERT INTO host_activity_reduced
			WITH previous_month AS (
				SELECT
					host,
					month_start_date,
					daily_hits,
					unique_visitors
				FROM host_activity_reduced
				WHERE month_start_date = month_start_param
			),
			today AS(
				SELECT
					host,
					DATE_TRUNC('month', event_time::DATE)::DATE AS month_start_date,
					event_time::DATE AS date,
					COALESCE(event_time::DATE - DATE_TRUNC('month', event_time::DATE)::DATE, 0) AS days_after_month_start,
					COUNT(1) AS hits,
					ARRAY_AGG(DISTINCT user_id) AS unique_visitors
				FROM
					events
				WHERE event_time::DATE = current_date_param
					AND user_id IS NOT NULL
				GROUP BY
					host,
					date
			)
			SELECT
				COALESCE(t.host, pm.host) AS host,
				COALESCE(pm.month_start_date, t.month_start_date) AS month_start_date,
				CASE
					WHEN pm.daily_hits IS NOT NULL
						THEN pm.daily_hits || ARRAY[COALESCE(hits,0)]
					WHEN pm.daily_hits IS NULL
						THEN ARRAY_FILL(0, ARRAY[COALESCE(days_after_month_start, 0)]) || ARRAY[COALESCE(hits,0)]
				END AS daily_hits,
				CASE
					WHEN pm.unique_visitors IS NULL
						THEN t.unique_visitors
					WHEN pm.unique_visitors IS NOT NULL
						THEN ARRAY(
							SELECT DISTINCT UNNEST(array_cat(pm.unique_visitors, t.unique_visitors))
						)
				END AS unique_visitors
			FROM today t
			FULL OUTER JOIN previous_month pm
				ON t.host = pm.host
			ON CONFLICT (host, month_start_date)
			DO
				UPDATE SET 
					daily_hits = EXCLUDED.daily_hits,
					unique_visitors = EXCLUDED.unique_visitors;
	END LOOP;
END $$;

SELECT * FROM host_activity_reduced;