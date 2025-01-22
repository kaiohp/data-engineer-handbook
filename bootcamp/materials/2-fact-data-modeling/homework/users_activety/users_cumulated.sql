DO $$
DECLARE
    start_date DATE;
    end_date DATE;
    current_date_param DATE;
BEGIN

    SELECT MIN(event_time::TIMESTAMP) INTO start_date FROM events;
    SELECT MAX(event_time::TIMESTAMP) INTO end_date FROM events;
	
    FOR current_date_param IN SELECT * FROM GENERATE_SERIES(start_date, end_date, '1 day'::INTERVAL) LOOP
		INSERT INTO users_cumulated
		WITH yesterday AS (
			SELECT
				*
			FROM users_cumulated
			WHERE date = current_date_param - '1 day'::INTERVAL
		),
		today AS (
			SELECT
				user_id::TEXT,
				DATE(CAST(event_time AS TIMESTAMP)) AS date_active
			FROM 
				events
			WHERE 
				DATE(CAST(event_time AS TIMESTAMP)) = current_date_param
				AND user_id IS NOT NULL
			GROUP BY 1, 2
		)
		SELECT 
			COALESCE(t.user_id, y.user_id),
			CASE
				WHEN y.dates_active IS NULL
					THEN COALESCE(ARRAY[t.date_active], ARRAY[]::DATE[])
				WHEN t.date_active IS NOT NULL
					THEN ARRAY[t.date_active] || y.dates_active
				ELSE y.dates_active
			END AS dates_active,
			COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
		FROM today t
		FULL OUTER JOIN yesterday y
		ON t.user_id = y.user_id;
	END LOOP;
END $$;

SELECT * FROM users_cumulated ORDER BY CARDINALITY(dates_active) DESC;
SELECT MIN(event_time::TIMESTAMP), MAX(event_time::TIMESTAMP) FROM events;