CREATE TYPE FILMS AS(
	film_id TEXT,
	film TEXT,
	year INTEGER,
	votes INTEGER,
	rating REAL
);

CREATE TYPE QUALITY_CLASS AS ENUM('star', 'good', 'average', 'bad');

CREATE TABLE actors (
	actor_id TEXT,
	actor TEXT,
	films FILMS[],
	quality_class QUALITY_CLASS,
	is_active BOOLEAN,
	current_year INTEGER,
	PRIMARY KEY (actor_id, current_year)
);