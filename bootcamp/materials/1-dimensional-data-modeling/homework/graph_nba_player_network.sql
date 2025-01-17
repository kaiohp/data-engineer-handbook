INSERT INTO edges
WITH deduped AS (
	SELECT 
		*,
		row_number() OVER (PARTITION BY game_id, player_id) AS row_num
	FROM game_details
)
SELECT
	player_id AS subject_identifier,
	'player'::vertex_type  AS subject_type,
	game_id AS object_identifier,
	'game'::vertex_type AS object_type,
	'plays_in'::edge_type AS edge_type,
	json_build_object(
		'start_position', start_position,
		'pts', pts,
		'team_id', team_id,
		'team_abbreviation', team_abbreviation
	) as properties
FROM deduped
WHERE row_num = 1;

INSERT INTO edges
WITH deduped AS (
	SELECT 
		*,
		row_number() OVER (PARTITION BY game_id, player_id) AS row_num
	FROM game_details
),
filtered AS (
	SELECT * FROM deduped WHERE row_num = 1
),
aggregated AS(
	SELECT
		f1.player_id as subject_player_id,
		MAX(f1.player_name) as subject_player_name,
		f2.player_id as object_player_id,
		MAX(f2.player_name) as object_player_name,
		CASE WHEN f1.team_id = f2.team_id THEN 'shares_team' ELSE 'plays_against' END::edge_type AS edge_type,
		COUNT(1) AS num_games,
		SUM(f1.pts) AS subject_points,
		SUM(f2.pts) AS object_points
	FROM filtered f1
	JOIN filtered f2
	ON f1.game_id = f2.game_id
	AND f1.player_id <> f2.player_id
	GROUP BY 
		f1.player_id,
		f2.player_id,
		5
)
SELECT 
	subject_player_id AS subject_identifier,
	'player'::vertex_type AS subject_type,
	object_player_id AS object_identifier,
	'player'::vertex_type AS object_type,
	edge_type,
	json_build_object(
		'num_games', num_games,
		'subject_points', subject_points,
		'object_points', object_points
	)
FROM aggregated;

SELECT 
	v.properties->>'player_name' AS player_name,
	MAX(CAST(e.properties->>'pts' AS INTEGER)) AS max_pts_in_game
FROM vertices v 
JOIN edges e
	ON e.subject_identifier = v.identifier
	AND e.subject_type = v.type
GROUP BY 1
ORDER BY 2 DESC;

SELECT 
	v.properties->>'player_name' AS player_name,
	CAST(v.properties->>'num_games' AS INTEGER)) AS number_of_games,
	CAST(v.properties->>'subject_points' AS INTEGER) AS subject_number_of_points,
FROM vertices v 
JOIN edges e
	ON e.subject_identifier = v.identifier
	AND e.subject_type = v.type
WHERE e.object_type = 'player'
GROUP BY 1
ORDER BY 2 DESC;
