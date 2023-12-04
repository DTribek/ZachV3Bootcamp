--Answers the following question:
--Which player scored the most points in one season?"
SELECT 
  player_name,
  season,
  total_points_score
  
FROM diegotribek.dashboard_game_table 
WHERE season <> 9999 and player_name <> 'overall'
ORDER BY total_points_score DESC 
LIMIT 1