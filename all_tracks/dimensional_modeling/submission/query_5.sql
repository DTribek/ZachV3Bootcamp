INSERT INTO diegotribek.actors_history_scd
/*Get historical information, considering 1919 as initial date, i.e., current_year = 1919*/
WITH last_year_scd AS (
    SELECT
      *
    FROM diegotribek.actors_history_scd
    WHERE current_year = 1919
),

/*Get actors info for current_year*/
current_year_scd AS (
  SELECT
    *
  FROM diegotribek.actors
  WHERE current_year = 1920
),

/*Combine the historical table with current date info*/
combined AS (
  SELECT
    COALESCE(ly.actor, cy.actor) AS actor,
    COALESCE(ly.actor_id, cy.actor_id) AS actor_id,
    CASE
      /*When both values didn't change*/
      WHEN ly.quality_class = cy.quality_class AND ly.is_active = cy.is_active 
        THEN 0
      /*When at least one of the values changed*/
      WHEN ly.quality_class <> cy.quality_class OR ly.is_active <> cy.is_active 
        THEN 1
      /*News users will get did_change = null*/
    END AS did_change,
    ly.quality_class AS ly_quality_class,
    cy.quality_class AS cy_quality_class,
    ly.is_active AS ly_is_active,
    cy.is_active AS cy_is_active,
    COALESCE(ly.start_date, cy.current_year) AS start_date,
    COALESCE(ly.end_date, cy.current_year) AS end_date,
    1920 AS current_year --fixed year, same as where clause in current_year_scd CTE
  FROM last_year_scd ly
  FULL OUTER JOIN current_year_scd cy 
  ON ly.actor_id = cy.actor_id
  AND ly.end_date + 1 = cy.current_year
),

/*Materializes the array based on did_change flag of previous CTE table*/
changes AS (
  SELECT
    actor,
    actor_id,
    current_year,
    CASE
    /*No changes so we just extend the end date*/
      WHEN did_change = 0 THEN ARRAY[
        ROW (
          ly_quality_class,
          ly_is_active,
          start_date,
          end_date + 1
        )
      ]
      /*Since the record change, we need to update the old one and append the new info*/
      WHEN did_change = 1 THEN ARRAY[
        ROW (
          ly_quality_class,
          ly_is_active,
          start_date,
          end_date
        ),
        ROW (
          cy_quality_class,
          cy_is_active,
          current_year,
          current_year
        )
      ]
      /*Covers the old people that couldn't change and new people (Reason for coalesce)*/
      WHEN did_change IS NULL THEN ARRAY[
        ROW (
          COALESCE(ly_quality_class, cy_quality_class),
          COALESCE(ly_is_active, cy_is_active),
          COALESCE(start_date, current_year),
          COALESCE(end_date, current_year)
        )
      ]
    END AS changes_array
  FROM combined
)

/*Append the information, unnesting the array*/
SELECT
  actor,
  actor_id,
  arr.quality_class,
  arr.is_active,
  arr.start_date,
  arr.end_date,
  current_year
FROM changes
CROSS JOIN UNNEST (changes_array) AS arr (quality_class, is_active, start_date, end_date)