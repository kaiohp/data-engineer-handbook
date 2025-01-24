CREATE TABLE user_devices_cumulated (
	user_id NUMERIC,
	browser_type TEXT,
	activity_datelist DATE[],
	date DATE,
	PRIMARY KEY (user_id, browser_type, date)
) PARTITION BY RANGE (date);

CREATE TABLE user_devices_cumulated_01_2023
	PARTITION OF user_devices_cumulated
	FOR VALUES FROM ('2023-01-01') TO ('2023-02-01')

CREATE INDEX idx_user_devices_cumulated_activity_dates ON user_devices_cumulated 
USING gin(activity_datelist);