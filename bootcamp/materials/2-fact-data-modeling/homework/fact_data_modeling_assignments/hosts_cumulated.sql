CREATE TABLE hosts_cumulated (
	host TEXT,
	activity_datelist DATE[],
	date DATE,
	PRIMARY KEY (host, date)
) PARTITION BY RANGE (date);

CREATE TABLE hosts_cumulated_01_2023
	PARTITION OF hosts_cumulated
	FOR VALUES FROM ('2023-01-01') TO ('2023-02-01');

CREATE INDEX idx_hosts_cumulated_activity_dates ON hosts_cumulated 
USING gin(activity_datelist);