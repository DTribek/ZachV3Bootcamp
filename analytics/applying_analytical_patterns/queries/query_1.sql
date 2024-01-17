INSERT INTO diegotribek.player_state_track
WITH yesterday as (
  SELECT *FROM diegotribek.player_state_track 
  WHERE season = 2021
)
  
,today as (
  SELECT 
    player_name,
    season
  FROM bootcamp.nba_player_seasons 
  WHERE season = 2022
)
  
SELECT 
  COALESCE(y.player_name, t.player_name) as player_name,
  COALESCE(y.first_active_season, t.season) as first_active_season,
  COALESCE(t.season, y.last_active_season) as last_active_season,
  CASE 
    WHEN y.player_name IS NULL
      THEN ARRAY[t.season] 
    WHEN t.player_name IS NULL
      THEN y.active_seasons
    ELSE
      t.season || y.active_seasons
  END AS active_seasons,
  CASE 
    WHEN y.player_name IS NULL
      THEN 'New'
    WHEN t.player_name IS NULL THEN
      CASE 
        WHEN y.player_state= 'Retired' or y.player_state= 'Stayed Retired'
          THEN 'Stayed Retired'
        ELSE
          'Retired'
      END
    WHEN y.player_state= 'Retired' or y.player_state= 'Stayed Retired'
      THEN  'Returned from Retirement'
    ELSE
      'Continued Playing'
  END as player_state,
  
  2022 as season
  
FROM
yesterday as y
FULL OUTER JOIN today as t 
ON y.player_name = t.player_name
  
