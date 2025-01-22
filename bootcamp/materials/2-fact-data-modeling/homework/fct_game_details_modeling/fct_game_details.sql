CREATE TABLE fct_game_details (
	dim_game_date DATE,
	dim_season INTEGER,
	dim_team_id INTEGER,
	dim_player_id INTEGER,
	dim_player_name TEXT,
	dim_start_position TEXT,
	dim_is_playing_at_home BOOLEAN,
	dim_did_team_wins BOOLEAN,
	dim_did_not_play BOOLEAN,
	dim_did_not_dress BOOLEAN,
	dim_not_with_team BOOLEAN,
	m_minutes REAL,
	m_fgm INTEGER,
	m_fga INTEGER,
	m_fg3m INTEGER,
	m_fg3a INTEGER,
	m_ftm INTEGER,
	m_fta INTEGER,
	m_oreb INTEGER,
	m_dreb INTEGER,
	m_reb INTEGER,
	m_ast INTEGER,
	m_stl INTEGER,
	m_blk INTEGER,
	m_turnovers INTEGER,
	m_pf INTEGER,
	m_pts INTEGER,
	m_plus_minus INTEGER,
	PRIMARY KEY(dim_game_date, dim_team_id, dim_player_id)
);

INSERT INTO fct_game_details
WITH with_row_number AS (
	SELECT
		g.game_date_est,
		g.season,
		g.home_team_id,
		g.home_team_wins,
		gd.*,
		ROW_NUMBER() OVER (PARTITION BY gd.game_id, gd.team_id, gd.player_id ORDER BY g.game_date_est) AS rn
	FROM
		game_details gd
	INNER JOIN games g
		ON gd.game_id = g.game_id
),
deduped AS (
SELECT
	game_date_est AS dim_game_date,
	season AS dim_season,
	team_id AS dim_team_id,
	player_id AS dim_player_id,
	player_name AS dim_player_name,
	start_position AS dim_start_position,
	team_id = home_team_id AS dim_is_playing_at_home,
	home_team_wins = 1 AND team_id = home_team_id AS dim_did_team_wins,
	COALESCE(POSITION('DNP' IN "comment"), 0) > 0 AS dim_did_not_play,
	COALESCE(POSITION('DND' IN "comment"), 0) > 0 AS dim_did_not_dress,
	COALESCE(POSITION('NWT' IN "comment"), 0) > 0 AS dim_not_with_team,
	CAST(SPLIT_PART("min", ':', 1) AS REAL)
		+ CAST(SPLIT_PART("min", ':', 2) AS REAL)/60 
		AS m_minutes,
	fgm	AS m_fgm,
	fga	AS m_fga,
	fg3m AS m_fg3m,
	fg3a AS m_fg3a,
	ftm	AS m_ftm,
	fta	AS m_fta,
	oreb AS m_oreb,
	dreb AS m_dreb,
	reb	AS m_reb,
	ast	AS m_ast,
	stl	AS m_stl,
	blk	AS m_blk,
	"TO" AS m_turnovers,
	pf AS m_pf,
	pts	AS m_pts,
	plus_minus AS m_plus_minus
FROM with_row_number
WHERE rn = 1
)
SELECT * FROM deduped;

SELECT * FROM fct_game_details;