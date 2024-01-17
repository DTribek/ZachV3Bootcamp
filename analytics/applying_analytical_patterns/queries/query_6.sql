--ANSWER: team: PHI, BOS, DET victories: 62
--Get team, victory (int) and game date.
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
    CASE 
      --Team home and home win
      WHEN (games.home_team_id = details.team_id AND games.home_team_wins = 1)
        THEN 1
      --Team visitor and visitor win 
      WHEN games.home_team_id <> details.team_id AND 
       games.home_team_wins <> 1
       THEN 1
     --Team loses
     ELSE 0
    END as winnings,
    games.game_date_est
    
  FROM dedup_game_details as details
  INNER JOIN dedup_nba_games as games 
  ON details.game_id = games.game_id

  WHERE games.row_number = 1 AND
  details.row_number = 1
)

--Get the sum of victories considering 90 game interval
,sum_table as (
  SELECT 
      team,
      SUM(winnings) OVER(ORDER BY game_date_est ROWS BETWEEN CURRENT ROW AND 89 FOLLOWING) as max_victories
  FROM base_data
)

--Use dense rank to get the first team/team(s) with max value
,ranked_table as (
  SELECT 
  team,
  max_victories,
  dense_rank() OVER(ORDER BY max_victories DESC) as ranked
  FROM sum_table
)
--Get the teams ranked with max victories considering the interval
SELECT 
  DISTINCT team,
  max_victories
FROM ranked_table
WHERE ranked = 1