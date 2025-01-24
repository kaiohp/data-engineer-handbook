WITH game_details_rn AS (
	SELECT 
		g.game_date_est AS game_date,
		g.season,
		g.home_team_id,
		g.home_team_wins,
		gd.*, 
		ROW_NUMBER() OVER (PARTITION BY gd.game_id, gd.team_id, gd.player_id ORDER BY g.game_date_est) as rn
	FROM game_details gd
	INNER JOIN games g ON gd.game_id = g.game_id
),
game_details_deduped AS (
	SELECT
		game_date,
		season,
		home_team_id,
		home_team_wins,
	    game_id,
	    team_id,
	    team_abbreviation,
	    team_city,
	    player_id,
	    player_name,
	    nickname,
	    start_position,
	    comment,
	    min,
	    fgm,
	    fga,
	    fg_pct,
	    fg3m,
	    fg3a,
	    fg3_pct,
	    ftm,
	    fta,
	    ft_pct,
	    oreb,
	    dreb,
	    reb,
	    ast,
	    stl,
	    blk,
	    "TO",
	    pf,
	    pts,
	    plus_minus
	FROM game_details_rn
	WHERE rn = 1
)
SELECT * FROM game_details_deduped;