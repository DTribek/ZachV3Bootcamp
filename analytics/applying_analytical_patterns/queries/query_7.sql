--ANSWER: 136 GAMES
--Get lebron data, considering 1 if the condition is met (10 score or more)
/*Uses row_number to deduplicate values. Considered primary_keys are: game_id, team_id, player_id*/
WITH dedup_game_details as (
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) as row_number
  FROM bootcamp.nba_game_details
)

,dedup_nba_games as (
  SELECT 
    game_date_est,
    game_id,
    ROW_NUMBER() OVER(PARTITION BY game_id ORDER BY game_date_est DESC) as row_number
    FROM bootcamp.nba_games
)

,lebron_data as (
  SELECT
    details.player_name,
    CASE 
      WHEN COALESCE(pts,0) > 10
        THEN 1
      ELSE 0
    END as current_game_score,
    games.game_date_est
    
  FROM dedup_game_details as details
  
  INNER JOIN dedup_nba_games  as games 
  ON details.game_id = games.game_id
  
  WHERE details.player_name = 'LeBron James' 
  AND details.row_number = 1 
  AND games.row_number = 1
)

--CTE to get the previous data condition met game
,lag_cte as (
  SELECT *,
  LAG(current_game_score, 1) OVER (ORDER BY game_date_est) as previous_game_score
  FROM lebron_data
)

--Build streak identifier, to use as partition in cumulative sum
,streak_cte as (
    SELECT *,
    SUM(CASE WHEN previous_game_score = current_game_score THEN 0 ELSE 1 END) OVER (ORDER BY game_date_est) as streak_identifier
    FROM lag_cte 
)

--Uses cumulative SUM to get the max value, considering streak_identifier as key
,games_10_score as (
  SELECT *,
  SUM(current_game_score) OVER(PARTITION BY streak_identifier ORDER BY game_date_est) as max_games_10_score
  FROM streak_cte
  ORDER BY game_date_est
)

--Answers the following question:
--How many games in a row did LeBron James score over 10 points a game?
SELECT 
  player_name,
  game_date_est,
  max(max_games_10_score) as total_games_10_score
FROM games_10_score 
GROUP BY player_name, game_date_est
ORDER BY total_games_10_score DESC
LIMIT 1