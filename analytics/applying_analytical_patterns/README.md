# Analytics Track Applying Analytical Patterns Homework

The homework for this week will be to write a total of 4 queries. We will use the `players`, `players_scd` and `player_seasons` tables from prior weeks.

- Write a query (`query_1`) that does state change tracking for `players`. Create a state change-tracking field that takes on the following values:
  - A player entering the league should be `New`
  - A player leaving the league should be `Retired`
  - A player staying in the league should be `Continued Playing`
  - A player that comes out of retirement should be `Returned from Retirement`
  - A player that stays out of the league should be `Stayed Retired`
  
- Write a query (`query_2`) that uses `GROUPING SETS` to perform aggregations of the `game_details` data. Create slices that aggregate along the following combinations of dimensions:
  - player and team
  - player and season
  - team
- Build additional queries on top of the results of the `GROUPING SETS` aggregations above to answer the following questions:
  - Write a query (`query_3`) to answer: "Which player scored the most points playing for a single team?"
  - Write a query (`query_4`) to answer: "Which player scored the most points in one season?"
  - Write a query (`query_5`) to answer: "Which team has won the most games"
- Write a query (`query_6`) that uses window functions on `game_details` to answer the question: "What is the most games a single team has won in a given 90-game stretch?"
- Write a query (`query_7`) that uses window functions on `game_details` to answer the question: "How many games in a row did LeBron James score over 10 points a game?"

Please add these queries into the corresponding files in the `queries` folder.
