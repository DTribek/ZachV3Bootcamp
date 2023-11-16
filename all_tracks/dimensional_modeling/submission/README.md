# All Tracks Dimensional Modeling Homework

The submission is divided in 5 queries. Let's go to a brief explanation for each one of them.


### Query_1

Creates the table `actors`, partitioned by `current_year`, 
with the following fields:

- `actor`: Actor name
- `actor_id`: Actor's ID [Primary Key]
- `films`: An array of `struct`, containing all the films done by actor until current_year, with the following fields:
  - `film`: The name of the film.
  - `votes`: The number of votes the film received.
  - `rating`: The rating of the film.
  - `film_id`: A unique identifier for each film.
- `quality_class`: A categorical bucketing of the average rating of the movies for this actor in their most recent year:
  - `star`: Average rating > 8.
  - `good`: Average rating > 7 and ≤ 8.
  - `average`: Average rating > 6 and ≤ 7.
  - `bad`: Average rating ≤ 6.
- `is_active`: A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
- `current_year`: The year this row represents for the actor


### Query_2

Populates the table actors with the information contained in the table bootcamp.actor_films.
For the first query run, both dates in `last_year` and `filtered_year` CTE's need to be the same, in this case 1914.

For incremental run, the user needs to increment `filtered_year` CTE date 
as (`last_year` CTE date + 1)

A result example is showed below:
![homework_1](https://github.com/DTribek/DiegoTribek/assets/46631262/612944a9-9fec-4209-852e-7ee0d8ecad5c)


### Query_3

Creates the table `actors_history_scd` as a Type 2 Slowly Changing Dimension Table,
partitioned by `current_year` with the following fields:

- `actor`: Actor name
- `quality_class`: A categorical bucketing of the average rating of the movies for this actor in their most recent year
- `is_active`:A BOOLEAN field that indicates whether an actor is currently active in the film industry (i.e., making films this year).
- `start_date`: Current year with both conditions (`quality_clas` and `is_active`) state begin 
- `end_date`Current year with both conditions (`quality_clas` and `is_active`) state ended.
- `current_year`: The year this row represents for the actor


### Query_4

Populates the table `actors_history_scd`, considering all years until 1919.
User's can change this date to any desidered year, but for incremental loads, 
it's recommended to use Query_5 since it's more performant.


### Query_5

Populate a single year's worth of the `actors_history_scd` table by combining the previous year's SCD data 
with the new incoming data from the `actors` table for this year.
Since the historical data is already loaded by Query_4, the user just need to change the dates in both CTE's
`last_year_scd` and `current_year_scd` and `combined` CTE `current_year` in line 38.