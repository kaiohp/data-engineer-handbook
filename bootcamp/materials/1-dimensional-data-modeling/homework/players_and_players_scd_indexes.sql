-- For simple equality filters
CREATE INDEX idx_players_current_season ON players(current_season);
CREATE INDEX idx_players_name ON players(player_name);

-- For multiple conditions used together
CREATE INDEX idx_players_scd_seasons 
ON players_scd(current_season, end_season);