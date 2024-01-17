--Answers the following question:
--Which team has won the most games"
--ANSWER: team: SAS, 14890 wins.
SELECT 
  team,
  season,
  total_wins
FROM diegotribek.dashboard_game_table 
WHERE season = 9999 and player_name = 'overall'
ORDER BY total_wins DESC 
LIMIT 1