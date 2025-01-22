WITH users_end_of_month AS (
	SELECT
		*
	FROM users_cumulated
	WHERE date = DATE('2023-01-31')
),
daily_dates AS (
SELECT 
	series_date::DATE,
	EXTRACT(DOW FROM series_date) AS day_of_week,
	31 - EXTRACT(DAY FROM series_date) AS position_from_end

FROM 
	GENERATE_SERIES(DATE('2023-01-01'), DATE('2023-01-31'), '1 day'::INTERVAL)
		AS series_date
),
weekend_weekday_masks AS (
    SELECT 
        -- Create weekend bitmask
        STRING_AGG(
            CASE 
                WHEN day_of_week IN (0, 6) THEN '1' -- Sunday (0) and Saturday (6)
                ELSE '0'
            END,
            '' ORDER BY series_date DESC
        )::BIT(32) AS weekend_mask,
        
        -- Create weekday bitmask
        STRING_AGG(
            CASE 
                WHEN day_of_week IN (0, 6) THEN '0' -- Sunday (0) and Saturday (6)
                ELSE '1'
            END,
            '' ORDER BY series_date DESC
        )::BIT(32) AS weekday_mask,
        
        -- Count total weekends and weekdays
        SUM(CASE WHEN day_of_week IN (0, 6) THEN 1 ELSE 0 END) as total_weekend_days,
        SUM(CASE WHEN day_of_week NOT IN (0, 6) THEN 1 ELSE 0 END) as total_weekdays
    FROM daily_dates
),
activity_position_values  AS (
	SELECT
		*,
		CASE
			WHEN dates_active @> ARRAY[series_date]
				THEN POW(2, 32 - (date - series_date))
				ELSE 0
			END AS activity_bit_position
	FROM users_end_of_month
	CROSS JOIN daily_dates
),
user_activity_bitmask AS (
SELECT
	user_id,
	date,
	EXTRACT(DAY FROM date) AS days_in_the_month,
	(SUM(activity_bit_position)::BIGINT)::BIT(32) AS activity_bitmask,
	'11111110000000000000000000000000'::BIT(32) AS recent_week_mask,
	'10000000000000000000000000000000'::BIT(32) AS latest_day_mask

FROM activity_position_values
GROUP BY user_id, date
)
SELECT
    user_id,
    activity_bitmask,
    BIT_COUNT(activity_bitmask) > 0 AS is_monthly_active,
    BIT_COUNT(recent_week_mask & activity_bitmask) > 0 AS is_weekly_active,
    BIT_COUNT(latest_day_mask & activity_bitmask) > 0 AS is_last_day_active,
    BIT_COUNT(activity_bitmask) >= days_in_the_month AS is_daily_active,
	BIT_COUNT(weekend_mask & activity_bitmask) AS weekend_days_active,
	BIT_COUNT(weekday_mask & activity_bitmask) AS weekday_days_active,
	(100.0 * BIT_COUNT(activity_bitmask) / days_in_the_month) AS activity_score
FROM user_activity_bitmask
CROSS JOIN weekend_weekday_masks
GROUP BY user_id, activity_bitmask, recent_week_mask, latest_day_mask, days_in_the_month, weekend_mask, weekday_mask;