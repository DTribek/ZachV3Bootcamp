CREATE TABLE diegotribek.hosts_cumulated (
  host VARCHAR,
  host_activity_datelist ARRAY<DATE>,
  event_date DATE
)
WITH (
  format = 'PARQUET', partitioning= ARRAY['event_date']
)