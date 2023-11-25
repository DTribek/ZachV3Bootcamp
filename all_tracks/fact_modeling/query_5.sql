INSERT INTO diegotribek.user_devices_cumulated
WITH old_data AS (
  SELECT
    *
  FROM diegotribek.user_devices_cumulated
  WHERE
  DATE(event_date) = DATE('2021-12-30')
),
  
new_data AS (
  SELECT
    *
  FROM bootcamp.web_events
  WHERE
  DATE(event_time) = DATE('2022-01-01')
)

,aggregated as (
  SELECT
    COALESCE(old_data.user_id, new_data.user_id) as user_id,
    CASE 
      /*Old records that didn't change*/
      WHEN old_data.user_id IS NOT NULL AND new_data.user_id IS NULL 
        THEN old_data.device_activity_datelist
      /*New records*/
      WHEN old_data.user_id IS NULL AND new_data.user_id IS NOT NULL 
        THEN MAP(ARRAY[CAST(new_data.device_id AS BIGINT)], ARRAY[ARRAY[DATE(new_data.event_time)]])        
  

      /*Records will change*/
      WHEN old_data.user_id IS NOT NULL AND new_data.user_id IS NOT NULL
        THEN 
           /*Check if the device is present in the list or not*/
            CASE 
                 /*Device is not present in the device list*/
                WHEN element_at(old_data.device_activity_datelist, new_data.device_id) IS NULL
                    THEN MAP(ARRAY[CAST(new_data.device_id AS BIGINT)], ARRAY[ARRAY[DATE(new_data.event_time)]])        
                /*Device is present in the device list*/
                ELSE
                    MAP(ARRAY[CAST(new_data.device_id AS BIGINT)], 
                        ARRAY[DATE(new_data.event_time) || element_at(old_data.device_activity_datelist, new_data.device_id)]) 
            END
    END device_activity_datelist,
    DATE('2022-01-01') as event_date
  FROM old_data
  FULL OUTER JOIN new_data 
  ON new_data.user_id = old_data.user_id
)

SELECT 
  user_id,
  map_union(device_activity_datelist) as device_activity_datelist
  event_date
FROM aggregated
  WHERE
    user_id = 749228935 
    GROUP BY user_id, event_date