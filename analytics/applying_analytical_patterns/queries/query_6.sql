--ANSWER: team: DAL, MIA, victories: 72
--Get team, victory (int) and game date.
WITH base_data as (
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
    
  FROM bootcamp.nba_game_details as details
  INNER JOIN bootcamp.nba_games as games 
  ON details.game_id = games.game_id
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