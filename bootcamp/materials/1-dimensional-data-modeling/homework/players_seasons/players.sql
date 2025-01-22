SELECT * FROM player_seasons;

CREATE TYPE season_stats AS (
	season INTEGER,
	gp INTEGER,
	pts REAL,
	reb REAL,
	ast REAL
);

CREATE TYPE scoring_class AS ENUM ('star', 'good', 'average', 'bad');

CREATE TABLE players (
	player_name TEXT,
	height TEXT,
	college TEXT,
	country TEXT,
	draft_year TEXT, 
	draft_round TEXT,
	draft_number TEXT,
	season_stats season_stats[],
	scoring_class scoring_class,
	years_since_last_season INTEGER,
	is_active BOOLEAN,
	current_season INTEGER,
	PRIMARY KEY(player_name, current_season)
);

DO $$
BEGIN
FOR s IN 1996..2022 LOOP
	INSERT INTO players 
	WITH yesterday AS (
		SELECT * FROM players
		WHERE current_season = s-1
	),
	today AS (
		SELECT * FROM player_seasons
		WHERE season = s
	)
	SELECT
		COALESCE(t.player_name, y.player_name) AS player_name,
		COALESCE(t.height, y.height) AS height,
		COALESCE(t.college, y.college) AS college,
		COALESCE(t.country, y.country) AS country,
		COALESCE(t.draft_year, y.draft_year) AS draft_year,
		COALESCE(t.draft_round, y.draft_round) AS draft_round,
		COALESCE(t.draft_number, y.draft_number) AS draft_number,
		CASE 
			WHEN y.season_stats IS NULL
				THEN ARRAY[
					ROW(
						t.season,
						t.gp,
						t.pts,
						t.reb,
						t.ast
					)::season_stats
				]
			WHEN t.season IS NOT NULL
				THEN y.season_stats || ARRAY[ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
			ELSE y.season_stats
		END AS season_stats,
		CASE
			WHEN t.season IS NOT NULL 
				THEN
					CASE
						WHEN t.pts > 20 THEN 'star'
						WHEN t.pts > 15  THEN 'good'
						WHEN t.pts > 10 THEN 'average'
						ELSE 'bad'
					END::scoring_class
				ELSE
					y.scoring_class
			END AS scoring_class,
			CASE 
				WHEN t.season IS NOT NULL 
					THEN 0
					ELSE y.years_since_last_season + 1
			END as years_since_last_season,
		t.season IS NOT NULL AS is_active,
		COALESCE(t.season, y.current_season + 1) as current_season
	FROM today t
	FULL OUTER JOIN yesterday y
	ON t.player_name = y.player_name;
END LOOP;
END $$;

WITH last_season AS (
SELECT max(current_season) as season FROM players
),
flatter_metrics AS (
SELECT 
	player_name,
	scoring_class,
	(season_stats[CARDINALITY(season_stats) -1]::season_stats).pts AS previus_season_pts,
	(season_stats[CARDINALITY(season_stats)]::season_stats).pts AS latest_season_pts
FROM players 
WHERE current_season = (SELECT season from last_season)
)
SELECT
	player_name,
	latest_season_pts,
	previus_season_pts,
	latest_season_pts/previus_season_pts AS performance, 
	latest_season_pts - previus_season_pts AS delta
FROM flatter_metrics
WHERE NULLIF(previus_season_pts,0) IS NOT NULL
AND scoring_class in ('star', 'good')
ORDER BY performance DESC;

SELECT * FROM players WHERE player_name = 'Michael Jordan';