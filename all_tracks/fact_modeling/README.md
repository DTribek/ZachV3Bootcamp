# All Tracks Fact Data Modeling Homework

For this week, we'll be using the `devices` and `events` datasets.

You should implement 5 out of the 8 following sections.

## De-dupe Query (query_1)

Write a query to de-duplicate the `game_details` table from the dimensional modeling week so there are no duplicate values.

## Cumulative User Devices DDL (query_2)

Write a DDL statement to create a table called `user_devices_cumulated` with a `device_activity_datelist` field. This field should look like a type `MAP<STRING, ARRAY[DATE]>`.

## Cumulative User Devices Date List Computation (query_3)

Write the cumulative query to generate the `device_activity_datelist` field from the `events` table.

## Cumulative User Devices Date List Int Computation (query_4)

Write the query to generate a `datelist_int` type column from the `device_activity_datelist` field in the previous query.

## Host Cumulative Activity DDL (query_5)

Write a DDL statement to create a `hosts_cumulated` table, containing a `host_activity_datelist` field that shows on which dates a given host saw activity.

## Host Cumulative Activity Incremental Query (query_6)

Write a query to incrementally generate the next value for the `host_activity_datelist` field.

## Reduced Host Activity DDL (query_7)

Write a DDL statement to create a monthly `host_activity_reduced` table, containing the fields:

- `month`
- `host`
- `hit_array` (think `COUNT(1)`)
- `unique_visitors_array` (think `COUNT(DISTINCT user_id)`)

## Reduced Host Activity Daily Incremental Query (query_8)

Write a query to incrementally generate the `host_activity_reduced` on a daily basis (the table is at the monthly level, but think you might want daily updates day after day for the most recent month).
