# All Tracks Dimensional Modeling Homework

This week's assignment involves working with the `actor_films` dataset. Your task is to construct a series of SQL queries and table definitions that will allow us to model the actor_films dataset in a way that facilitates efficient analysis. This involves creating new tables, defining data types, and writing queries to populate these tables with data from the actor_films dataset

## Dataset Overview

The `actor_films` dataset contains the following fields:

- `actor`: The name of the actor.
- `actorid`: A unique identifier for each actor.
- `film`: The name of the film.
- `year`: The year the film was released.
- `votes`: The number of votes the film received.
- `rating`: The rating of the film.
- `filmid`: A unique identifier for each film.

The primary key for this dataset is (`actor_id`, `film_id`).

## Assignment Tasks

### Actors Table DDL (query_1)

Create a DDL for the `actors` table with the following fields:

- `actor`: Actor name
- `actorid`: Actor's ID
- `films`: An array of `struct` with the following fields:
  - `film`: The name of the film.
  - `votes`: The number of votes the film received.
  - `rating`: The rating of the film.
  - `filmid`: A unique identifier for each film.
- `quality_class`: A categorical bucketing of the average rating of the movies for this actor in their most recent year:
  - `star`: Average rating > 8.
  - `good`: Average rating > 7 and ≤ 8.
  - `average`: Average rating > 6 and ≤ 7.
  - `bad`: Average rating ≤ 6.
- `is_active`: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
- `current_year`: The year this row represents for the actor

### Cumulative Table Computation Query (query_2)

Write a query that populates the `actors` table one year at a time.

### Actors History SCD Table DDL (query_3)

Create a DDL for an `actors_history_scd` that tracks the following fields for each actor in the `actors` table:

- `quality_class`
- `is_active`
- `start_date`
- `end_date`

Note that this table should be appropriately modeled as a Type 2 Slowly Changing Dimension Table (`start_date` and `end_date`).

### Actors History SCD Table Batch Backfill Query (query_4)

Write a "backfill" query that can populate the entire `actors_history_scd` table in a single query.

### Actors History SCD Table Incremental Backfill Query (query_5)

Write an "incremental" query that can populate a single year's worth of the `actors_history_scd` table by combining the previous year's SCD data with the new incoming data from the `actors` table for this year.

## Submission Guidelines

Place your SQL into the corresponding query file in the `submission` folder.
