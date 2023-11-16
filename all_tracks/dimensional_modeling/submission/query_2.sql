INSERT INTO diegotribek.actors
/*Get the information from last year. Just need to replace the where clause*/
WITH last_year as (
  SELECT
    *
  FROM diegotribek.actors
  WHERE current_year = 1914
),

/*Filter the year that we want to add data*/
filtered_year as (
  SELECT *
  FROM bootcamp.actor_films
  WHERE YEAR = 1914
),

/*Compute the avg weighted rating per actor, during the selected year*/
avg_rating as (
  SELECT 
    actor_id,
    ROUND(SUM(votes*rating)/ SUM(votes),2) as avg_rating
  FROM filtered_year 
  GROUP BY actor_id
),

/*Add avg_rating per actor, grouping the films per actor and year*/
this_year as (
  SELECT 
    t1.actor as actor,
    t1.actor_id as actor_id,
    t1.year as year,
    ARRAY_AGG(ROW(t1.film, t1.votes, t1.rating, t1.film_id)) as films,
    t2.avg_rating as avg_rating
  FROM  filtered_year as t1
  INNER JOIN avg_rating as t2
  ON t1.actor_id = t2.actor_id
  GROUP BY t1.actor, t1.actor_id, t1.year , t2.avg_rating
)

/*Join the information from current and previous year*/
SELECT
  COALESCE(ty.actor, ly.actor) as actor,
  COALESCE(ty.actor_id, ly.actor_id) as actor_id,
  CASE 
    WHEN ty.year IS NULL THEN 
      ly.films
    WHEN ly.current_year IS NULL THEN 
      ty.films
    WHEN ly.current_year IS NOT NULL THEN
      ty.films || ly.films
  END as films,
  CASE 
    WHEN ty.year IS NULL THEN ly.quality_class
    WHEN ty.year IS NOT NULL THEN
      CASE 
        WHEN avg_rating > 8 THEN 'star'
        WHEN avg_rating > 7 THEN 'good'
        WHEN avg_rating > 6 THEN 'average'
        WHEN avg_rating <= 6 THEN 'bad'
      END
  END as quality_class,
  
  ty.year IS NOT NULL as is_active,
  COALESCE(ty.year, ly.current_year + 1) as current_year
FROM last_year ly

FULL OUTER JOIN this_year ty
ON ly.actor_id = ty.actor_id
