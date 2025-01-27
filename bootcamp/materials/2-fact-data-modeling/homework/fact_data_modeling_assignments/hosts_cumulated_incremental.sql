DO $$
DECLARE
	start_date_param DATE;
	end_date_param DATE;
	current_date_param DATE;
BEGIN
	
	SELECT MIN(event_time::DATE) INTO start_date_param FROM events;
	SELECT MAX(event_time::DATE) INTO end_date_param FROM events;
	
	FOR current_date_param IN SELECT * FROM GENERATE_SERIES(start_date_param, end_date_param, '1 day'::INTERVAl) LOOP
		INSERT INTO hosts_cumulated
			WITH yesterday AS (
				SELECT
					host,
					activity_datelist,
					date
				FROM
					hosts_cumulated
				WHERE date = current_date_param - '1 day'::INTERVAL
			),
			today AS (
				SELECT host, event_time::DATE AS date
				FROM events
				WHERE event_time::DATE = current_date_param
				GROUP BY
				host, 
				date
			)
			SELECT
				COALESCE(t.host, y.host) AS host,
				CASE
					WHEN y.activity_datelist IS NULL
						THEN ARRAY[t.date]
					WHEN t.date IS NOT NULL
						THEN y.activity_datelist || ARRAY[t.date]
					ELSE y.activity_datelist
				END AS activity_datelist,
				COALESCE(t.date, y.date + '1 day'::INTERVAL) AS date
			FROM today t
			FULL OUTER JOIN yesterday y
				ON y.host = t.host;
	END LOOP;
END $$;
