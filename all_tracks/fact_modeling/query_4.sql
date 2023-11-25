/*Initiallly unnest the arr to get the dates for each device in the last day.*/
WITH initial_data as (
  SELECT 
    user_id,
    arr.device_id,
    arr.date_list,
    event_date 
  FROM diegotribek.user_devices_cumulated
  CROSS JOIN UNNEST (device_activity_datelist) AS arr (device_id, date_list) 
  WHERE
  event_date = DATE('2022-01-31')
)


/*Calculate the integer, based on day 2022-01-31*/
  SELECT 
    user_id,
    device_id,
    CAST(SUM(CASE 
      WHEN CONTAINS(date_list, sequence_date)
        THEN POW(2, 30 - DATE_DIFF('day', sequence_date, event_date ))
      ELSE 0
    END) AS BIGINT) as datelist_int,
    event_date

  FROM initial_data 
  CROSS JOIN UNNEST (SEQUENCE(DATE('2022-01-01'), DATE('2022-01-31'))) as t(sequence_date)
  GROUP BY user_id, device_id, event_date


