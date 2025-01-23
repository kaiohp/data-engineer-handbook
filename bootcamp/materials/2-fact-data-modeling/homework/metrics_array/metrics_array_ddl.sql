CREATE TABLE metrics_array (
	user_id NUMERIC,
	month_start DATE,
	metric_name TEXT,
	metric_array REAL[],
	PRIMARY KEY (user_id, month_start, metric_name)
);