INSERT INTO diegotribek.user_devices_cumulated
/*CTE with all data from previous day*/
WITH old_data AS (
  SELECT
    *
  FROM diegotribek.user_devices_cumulated
  WHERE
  DATE(event_date) = DATE('2022-01-04')
),
  
/*Get Data from current day*/
new_data AS (
  SELECT
    web.user_id,
    DATE(web.event_time) as event_date,
    MAP_AGG(dev.browser_type, ARRAY[CAST(web.event_time AS date)]) AS device_activity_datelist
        
  FROM bootcamp.web_events as web
  
  LEFT JOIN bootcamp.devices as dev
  ON web.device_id = dev.device_id
  
  WHERE
  DATE(event_time) = DATE('2022-01-05')
  GROUP BY 1, 2
)

/*Aggregate old data with new data*/
,aggregated as (
  SELECT
    COALESCE(old_data.user_id, new_data.user_id) as user_id,
    CASE 

      /*Old records that didn't change*/
      WHEN old_data.user_id IS NOT NULL AND new_data.user_id IS NULL 
        THEN old_data.device_activity_datelist
      
      /*New records*/
      WHEN old_data.user_id IS NULL AND new_data.user_id IS NOT NULL 
        THEN new_data.device_activity_datelist

      ELSE
          MAP_ZIP_WITH(
                      COALESCE(old_data.device_activity_datelist, MAP()),
                      COALESCE(new_data.device_activity_datelist, MAP()),
                      (_, v1, v2) -> COALESCE(v1, ARRAY[]) || COALESCE(v2, ARRAY[])
          )        
    END device_activity_datelist,
    DATE('2022-01-05') as event_date
  FROM old_data
  FULL OUTER JOIN new_data 
  ON new_data.user_id = old_data.user_id
)

/*Final select, aggregate the data to remove duplicates and get completeness*/
SELECT 
  user_id,
  map_union(device_activity_datelist) as device_activity_datelist,
  event_date
FROM aggregated 
GROUP BY user_id, event_date