INSERT INTO diegotribek.actors_history_scd
/*Get historical information, considering 1919 as initial date, i.e., current_year = 1919*/
WITH actors_lagged as (
  SELECT 
    actor,
    actor_id,
    quality_class,
    is_active,
    LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) as is_active_last_year,
    LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) as quality_class_last_year,
    current_year
  FROM diegotribek.actors
  WHERE current_year <= 1919
),

/*Get the changes in "is_active" and "quality_class" column*/
streaked as (
  SELECT *,
  SUM(CASE WHEN is_active <> is_active_last_year THEN 1 ELSE 0 END) OVER (PARTITION BY actor_id ORDER BY current_year) as streak_active_identifier,
  SUM(CASE WHEN quality_class <> quality_class_last_year THEN 1 ELSE 0 END) OVER(PARTITION BY actor_id ORDER BY current_year) as streak_quality_class_identifier
  FROM actors_lagged 
)

/*Pull all information together, grouping by the actor and the changes identified in
the previous table*/
SELECT 
  actor,
  actor_id,
  MAX(quality_class) as quality_class,
  MAX(is_active) as is_active,
  MIN(current_year) as start_date,
  MAX(current_year) as end_date,
  1919 as current_year --Fixed value (considered in where clause, first table)
FROM streaked 
GROUP BY  
  actor,
  actor_id,
  streak_active_identifier,
  streak_quality_class_identifier