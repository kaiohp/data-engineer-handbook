DO $$
DECLARE
	start_date_param DATE;
	end_date_param DATE;
	current_date_param DATE;
BEGIN

	SELECT MIN(event_time::TIMESTAMP) INTO start_date_param FROM events;
    SELECT MAX(event_time::TIMESTAMP) INTO end_date_param FROM events;
	
	FOR current_date_param IN SELECT * FROM GENERATE_SERIES(start_date_param, end_date_param, '1 day'::INTERVAL) LOOP
		INSERT INTO user_devices_cumulated
			WITH yesterday AS(
				SELECT
					user_id,
					browser_type,
					activity_datelist,
					date
				FROM
					user_devices_cumulated
				WHERE
					date = current_date_param - '1 day'::INTERVAL
			),
			today AS (
				SELECT
					e.user_id,
					d.browser_type,
					e.event_time::DATE AS date
				FROM events e
				INNER JOIN devices d
					ON e.device_id = d.device_id
				WHERE e.user_id IS NOT NULL
					AND e.event_time::DATE = current_date_param
				GROUP BY e.user_id, d.browser_type, date
			)
			SELECT
				COALESCE(t.user_id, y.user_id) AS user_id,
				COALESCE(t.browser_type, y.browser_type) AS browser_type,
				CASE
					WHEN y.activity_datelist IS NULL
						THEN ARRAY[t.date]
					WHEN t.date IS NOT NULL
						THEN ARRAY[t.date] || y.activity_datelist
					ELSE y.activity_datelist
				END AS activity_datelist,
				COALESCE(t.date, y.date + '1 day'::INTERVAL) as date
			FROM today t
			FULL OUTER JOIN yesterday y
				ON t.user_id = y.user_id
					AND t.browser_type = y.browser_type;
	END LOOP;
END $$;

SELECT * FROM user_devices_cumulated ORDER BY CARDINALITY(activity_datelist) DESC;