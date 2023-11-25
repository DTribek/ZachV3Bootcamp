CREATE TABLE diegotribek.user_devices_cumulated (
  user_id BIGINT,
  device_activity_datelist MAP(BIGINT, ARRAY<DATE>),
  event_date DATE
)
WITH (
  format = 'PARQUET', partitioning= ARRAY['event_date']
)