--------------------------------------------------------------
--Query to create dashboard_game_table
--------------------------------------------------------------
CREATE TABLE diegotribek.dashboard_game_table (
    team VARCHAR,
    player_name VARCHAR,
    season INT,
    total_points_score INT,
    total_wins INT
  )
WITH
  (FORMAT = 'PARQUET', partitioning = ARRAY['season','team'])

--------------------------------------------------------------
--Insert results in the table created above
--------------------------------------------------------------
INSERT INTO diegotribek.dashboard_game_table
--Enhanced the data from nba_game_details
WITH base_data as (
  SELECT
    details.team_abbreviation as team,
    details.player_id,
    details.player_name,
    COALESCE(details.pts, 0) as pts,
    CASE 
      WHEN games.home_team_id = details.team_id
        THEN 'home'
      ELSE
        'visitor'
    END as match_category,
    CASE 
      WHEN games.home_team_wins = 1
        THEN 'home_win'
      ELSE
        'visitor_win'
    END as match_result,
    games.season as season
    
  FROM bootcamp.nba_game_details as details
  INNER JOIN bootcamp.nba_games as games 
  ON details.game_id = games.game_id
)

--Use grouping sets to get the result accordingly
SELECT 
  COALESCE(team, 'overall') as team,
  COALESCE(player_name, 'overall') as player_name,
  COALESCE(season, 9999) as season,
  SUM(pts) as total_points_scored,
  CASE 
    WHEN (match_category='home' and  match_result='home_win') or 
         (match_category='visitor' and  match_result='visitor_win')
         THEN 1
    ELSE 0
  END as team_wins
FROM base_data
GROUP BY GROUPING SETS (
  (player_name, team),
  (player_name, season),
  (team)
)
--------------------------------------------------------------
--Get the results
--------------------------------------------------------------
SELECT *FROM diegotribek.dashboard_game_table 