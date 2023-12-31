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
WITH dedup_game_details as (
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) as row_number
  FROM bootcamp.nba_game_details
)

,dedup_nba_games as (
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY game_id ORDER BY game_date_est DESC) as row_number
    FROM bootcamp.nba_games
)

,base_data as (
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
    
  FROM dedup_game_details as details
  INNER JOIN dedup_nba_games  as games 
  ON details.game_id = games.game_id
  
  WHERE games.row_number=1 AND
  details.row_number = 1
)

--Use grouping sets to get the result accordingly
,temp as (
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
GROUP BY 1,2,3,5
)

SELECT 
  team,
  player_name,
  season,
  total_points_scored,
  team_wins
FROM temp
GROUP BY GROUPING SETS (
  (team, total_points_scored, team_wins),
  (player_name, team),
  (player_name, season),
  (team)
)
--------------------------------------------------------------
--Get the results
--------------------------------------------------------------
SELECT *FROM diegotribek.dashboard_game_table 