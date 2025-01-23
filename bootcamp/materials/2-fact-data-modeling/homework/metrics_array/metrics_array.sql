DO $$
DECLARE
	month_start_param DATE := '2023-01-01'::DATE;
	month_end_param DATE := (month_start_param + INTERVAL '1 month' - INTERVAL '1 day')::DATE;
	current_date_param DATE;

BEGIN
	FOR current_date_param IN SELECT * FROM GENERATE_SERIES(month_start_param, month_end_param, '1 day'::INTERVAL) LOOP
		INSERT INTO metrics_array
			WITH yesterday_array AS (
				SELECT 
					user_id,
					month_start,
					metric_name,
					metric_array
				FROM metrics_array
				WHERE month_start = month_start_param
				AND metric_name = 'site_hits'
			),
			today_aggregate AS (
				SELECT
					user_id,
					DATE(event_time) AS date,
					COUNT(1) AS num_site_hits
				FROM events
				WHERE DATE(event_time) = current_date_param
				AND user_id IS NOT NULL
				GROUP BY user_id, date
			)
			SELECT
				COALESCE(t.user_id, y.user_id) AS user_id,
				COALESCE(y.month_start, DATE_TRUNC('month' , t.date))::DATE AS month_start,
				'site_hits' AS metric_name,
				CASE 
					WHEN y.metric_array IS NOT NULL
						THEN y.metric_array || ARRAY[COALESCE(t.num_site_hits, 0)]
					WHEN y.metric_array IS NULL
						THEN 
							ARRAY_FILL(0, ARRAY[COALESCE(date - DATE_TRUNC('month' , t.date)::DATE, 0)]) 
							|| ARRAY[COALESCE(t.num_site_hits, 0)]
					END AS metric_array
			FROM today_aggregate t
			FULL OUTER JOIN yesterday_array y
				ON t.user_id = y.user_id
			ON CONFLICT (user_id, month_start, metric_name)
			DO
				UPDATE SET metric_array = EXCLUDED.metric_array;
	END LOOP;
END $$;

SELECT * FROM metrics_array;
SELECT CARDINALITY(metric_array) AS array_length, COUNT(1) FROM metrics_array GROUP BY 1;

EXPLAIN ANALYSE
WITH daily_aggregated AS (
    SELECT
		month_start,
		metric_name,
        ARRAY[
            SUM(metric_array[1]),
            SUM(metric_array[2]),
            SUM(metric_array[3]),
            SUM(metric_array[4]),
            SUM(metric_array[5]),
            SUM(metric_array[6]),
            SUM(metric_array[7]),
            SUM(metric_array[8]),
            SUM(metric_array[9]),
            SUM(metric_array[10]),
            SUM(metric_array[11]),
            SUM(metric_array[12]),
            SUM(metric_array[13]),
            SUM(metric_array[14]),
            SUM(metric_array[15]),
            SUM(metric_array[16]),
            SUM(metric_array[17]),
            SUM(metric_array[18]),
            SUM(metric_array[19]),
            SUM(metric_array[20]),
            SUM(metric_array[21]),
            SUM(metric_array[22]),
            SUM(metric_array[23]),
            SUM(metric_array[24]),
            SUM(metric_array[25]),
            SUM(metric_array[26]),
            SUM(metric_array[27]),
            SUM(metric_array[28]),
            SUM(metric_array[29]),
            SUM(metric_array[30]),
            SUM(metric_array[31])
        ] as sum_metric_array
    FROM metrics_array
    GROUP BY month_start, metric_name
)
SELECT
	(month_start + ((index - 1)::TEXT || ' day')::INTERVAL)::DATE AS date,
	metric_name,
	elem AS value
FROM daily_aggregated,
LATERAL UNNEST(sum_metric_array) WITH ORDINALITY AS a(elem, index);

EXPLAIN ANALYSE
SELECT
	DATE(event_time) AS date,
	'site_hits' AS metric_name,
	COUNT(1) AS num_site_hits
FROM events
WHERE DATE(event_time) BETWEEN '2023-01-01'::DATE AND '2023-01-31'::DATE
	AND user_id IS NOT NULL
GROUP BY date