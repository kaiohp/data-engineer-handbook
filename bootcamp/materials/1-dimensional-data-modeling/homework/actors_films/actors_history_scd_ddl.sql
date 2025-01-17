CREATE TABLE actors_history_scd (
	actor_id TEXT,
	actor TEXT,
	quality_class QUALITY_CLASS,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER,
	current_year INTEGER,
	PRIMARY KEY (actor_id, start_year, end_year, current_year)
);

CREATE TYPE actors_history_scd_type AS(
	quality_class QUALITY_CLASS,
	is_active BOOLEAN,
	start_year INTEGER,
	end_year INTEGER
);