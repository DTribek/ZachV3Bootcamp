--Initiallly unnest the arr to get the dates for each device in the last day
WITH initial_data as (
  SELECT 
    user_id,
    arr.browser_info,
    arr.date_list,
    event_date 
  FROM diegotribek.user_devices_cumulated
  CROSS JOIN UNNEST (device_activity_datelist) AS arr (browser_info, date_list) 
  WHERE
  event_date = DATE('2022-01-31')
)

--Get the value in binary for each combination of user_id + browser_info.
SELECT 
  user_id,
  browser_info,
  LPAD(TO_BASE(CAST(SUM(CASE 
    WHEN CONTAINS(date_list, sequence_date)
      THEN POW(2, 31 - DATE_DIFF('day', sequence_date, event_date ))
    ELSE 0
  END) AS BIGINT), 2), 32, '0') as datelist_int,
  event_date

FROM initial_data 
CROSS JOIN UNNEST (SEQUENCE(DATE('2022-01-01'), DATE('2022-01-31'))) as t(sequence_date)
GROUP BY user_id, browser_info, event_date
