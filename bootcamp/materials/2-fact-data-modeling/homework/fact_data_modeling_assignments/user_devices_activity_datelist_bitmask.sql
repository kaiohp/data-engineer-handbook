WITH user_devices_cumulated_end_of_month AS (
	SELECT
		user_id,
		browser_type,
		activity_datelist,
		date
	FROM user_devices_cumulated
	WHERE date = '2023-01-31'::DATE
),
dayle_dates AS (
	SELECT
		date_series::DATE,
		EXTRACT(DOW FROM date_series) AS day_of_week,
		31 - EXTRACT(DAY FROM date_series) AS position_from_end
	FROM 
		GENERATE_SERIES('2023-01-01'::DATE, '2023-01-31'::DATE, '1 day'::INTERVAL) AS date_series
),
weekend_weekday_masks AS(
	SELECT
		STRING_AGG(
			CASE
				WHEN day_of_week IN (0, 6) 
					THEN '1'
				ELSE '0'
			END,
			'' ORDER BY date_series DESC
		)::BIT(32) AS weekend_mask,
		STRING_AGG(
			CASE
				WHEN day_of_week NOT IN (0, 6)
					THEN '1'
				ELSE '0'
			END,
			'' ORDER BY date_series DESC
		)::BIT(32) AS weekday_mask,
		SUM(CASE WHEN day_of_week IN (0, 6) THEN 1 ELSE 0 END) as total_weekend_days,
		SUM(CASE WHEN day_of_week NOT IN (0, 6) THEN 1 ELSE 0 END) as total_weekdays
	FROM
		dayle_dates
),
user_devices_bit_position AS (
	SELECT
		user_id,
		browser_type,
		date,
		CASE
			WHEN activity_datelist @> ARRAY[date_series]
				THEN POW(2, 32 - (date - date_series))
			ELSE 0
		END AS activity_bit_position
	FROM user_devices_cumulated_end_of_month
	CROSS JOIN dayle_dates
),
user_devices_bitmask AS (
SELECT
	user_id,
	browser_type,
	date,
	(SUM(activity_bit_position)::BIGINT)::BIT(32) AS activity_bitmask,
	EXTRACT(DAY FROM date) AS days_in_the_month,
	'11111110000000000000000000000000'::BIT(32) AS last_seven_days_mask,
	'10000000000000000000000000000000'::BIT(32) AS latest_day_mask
FROM user_devices_bit_position
GROUP BY
	user_id,
	browser_type,
	date
)
SELECT
	user_id,
	browser_type,
	date,
	activity_bitmask,
	BIT_COUNT(activity_bitmask) > 0 AS is_monthly_active,
	BIT_COUNT(last_seven_days_mask & activity_bitmask) > 0 AS is_last_seven_days_active,
	BIT_COUNT(latest_day_mask & activity_bitmask) > 0 AS is_last_day_active,
	BIT_COUNT(activity_bitmask) >= days_in_the_month AS is_daily_active,
	BIT_COUNT(weekend_mask & activity_bitmask) > 0 AS is_weekend_active,
	BIT_COUNT(weekday_mask & activity_bitmask) > 0 AS is_weekend_active,
	(100.0 * BIT_COUNT(activity_bitmask) / days_in_the_month) AS activity_score
FROM user_devices_bitmask
CROSS JOIN weekend_weekday_masks
GROUP BY
user_id,
browser_type,
date,
activity_bitmask,
last_seven_days_mask,
latest_day_mask,
days_in_the_month,
weekend_mask,
weekday_mask;