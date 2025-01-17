INSERT INTO vertices
SELECT 
	game_id AS identifier,
	'game'::vertex_type AS type,
	json_build_object(
		'pts_home', pts_home,
		'pts_away', pts_away,
		'winning_team', CASE WHEN home_team_wins = 1 THEN home_team_id ELSE visitor_team_id END
	) as properties
FROM games;

INSERT INTO vertices
WITH players_agg AS (
SELECT 
	player_id AS identifier,
	MAX(player_name) AS player_name,
	COUNT(1) AS number_of_games,
	SUM(pts) AS total_points,
	ARRAY_AGG(DISTINCT team_id) AS teams
FROM game_details
GROUP BY player_id
)
SELECT 
	identifier,
	'player'::vertex_type,
	json_build_object(
		'player_name', player_name,
		'number_of_games', number_of_games,
		'total_points', total_points,
		'teams', teams
	) as properties
FROM players_agg;

INSERT INTO vertices
WITH teams_agg AS (
 SELECT 
 	team_id AS identifier,
	abbreviation,
	nickname,
	city,
	arena,
	yearfounded AS year_founded
FROM teams
GROUP BY
 	team_id,
	abbreviation,
	nickname,
	city,
	arena,
	yearfounded
)
SELECT
	identifier,
	'team'::vertex_type AS type,
	json_build_object(
		'abbreviation', abbreviation,
		'nickname', nickname,
		'city', city,
		'arena', arena,
		'year_founded', year_founded
	) AS properties
FROM teams_agg;