/*Uses row_number to deduplicate values. Considered primary_keys are: game_id, team_id, player_id*/
WITH row_values as (
  SELECT 
    *,
    ROW_NUMBER() OVER(PARTITION BY game_id, team_id, player_id) as row_number
  FROM bootcamp.nba_game_details
)

/*Exclude duplicated data, i.e, data with row_number > 1.*/
SELECT 
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
  fgm
FROM row_values 
WHERE row_number=1