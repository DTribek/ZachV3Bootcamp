--Answers the following question:
--Which player scored the most points playing for a single team?"
SELECT 
  player_name,
  team,
  total_points_score
FROM diegotribek.dashboard_game_table 
WHERE season = 9999 and player_name <> 'overall'
ORDER BY total_points_score DESC 
LIMIT 1