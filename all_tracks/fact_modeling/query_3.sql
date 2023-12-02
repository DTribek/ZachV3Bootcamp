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
day_data AS (
  SELECT
    web.user_id,
    CAST(dev.browser_type as VARCHAR) as device_info,
    DATE(web.event_time) as event_date

  FROM bootcamp.web_events as web
  
  LEFT JOIN bootcamp.devices as dev
  ON web.device_id = dev.device_id
  
  WHERE
  DATE(event_time) = DATE('2022-01-05')
  GROUP BY 1, 2, 3
)

/*CTE to get all devices in array*/
,new_data as (
  SELECT 
    new.user_id,
    new.device_info,
    new.event_date,
    ARRAY_AGG(new_2.device_info) as device_array

  FROM day_data new
  INNER JOIN day_data new_2
  ON new.user_id = new_2.user_id

  GROUP BY 1,2,3
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
        THEN MAP(ARRAY[new_data.device_info], ARRAY[ARRAY[new_data.event_date]])        
  

      /*Records will change*/
      WHEN old_data.user_id IS NOT NULL AND new_data.user_id IS NOT NULL
        THEN 
           /*Check if the device is present in the list or not*/
            CASE 
                 /*Device is not present in the device list*/
                WHEN element_at(old_data.device_activity_datelist, new_data.device_info) IS NULL
                    THEN 
                      /*If all other devices have data today, just map the current device*/ 
                      CASE WHEN CARDINALITY(ARRAY_EXCEPT(map_keys(old_data.device_activity_datelist), new_data.device_array)) = 0
                        THEN MAP(ARRAY[new_data.device_info], ARRAY[ARRAY[new_data.event_date]]) 
                         /*Concat current device data with old devices data*/
                         ELSE
                            map_concat(MAP(ARRAY[new_data.device_info], ARRAY[ARRAY[new_data.event_date]]),
                              map_filter(old_data.device_activity_datelist, 
                              (k, v) -> CONTAINS(ARRAY_EXCEPT(map_keys(old_data.device_activity_datelist), new_data.device_array), k)
                              )
                            )      
                      END
                /*Device is present in the device list and every other device is in today's data*/
                WHEN CARDINALITY(ARRAY_EXCEPT(map_keys(old_data.device_activity_datelist), new_data.device_array)) = 0
                  THEN MAP(ARRAY[new_data.device_info], 
                           ARRAY[DATE(new_data.event_date) || element_at(old_data.device_activity_datelist, new_data.device_info)])
                  /*Deviceinfo is present in the device list but some device is not in today's data. 
                  Concat the present device data with old data from devices that don't appear in current's date*/
                  ELSE
                    map_concat(MAP(ARRAY[new_data.device_info], 
                      ARRAY[DATE(new_data.event_date) || element_at(old_data.device_activity_datelist, new_data.device_info)]),
                      map_filter(old_data.device_activity_datelist, 
                      (k, v) -> CONTAINS(ARRAY_EXCEPT(map_keys(old_data.device_activity_datelist), new_data.device_array), k)
                      )
                  )
            END
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