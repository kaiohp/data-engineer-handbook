CREATE TABLE host_activity_reduced (
	host TEXT NOT NULL,
	month_start_date DATE NOT NULL,
	daily_hits INTEGER[],
	unique_visitors NUMERIC[],
	PRIMARY KEY(host, month_start_date)
);

CREATE INDEX idx_host_activity_reduced_daily_hits ON host_activity_reduced USING GIN (daily_hits);
CREATE INDEX idx_host_activity_reduced_unique_visitors ON host_activity_reduced USING GIN (unique_visitors);