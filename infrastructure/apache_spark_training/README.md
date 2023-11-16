# Infrastructure Track Apache Spark Training Homework

In this homework, we're going to work with Spark to answer particular analytical questions, and we're going to analyze the effect of partitioning and sort order on the resulting data size.

We'll be working with the Halo medals dataset, which has the following fields:

- match_details: a row for every players performance in a match
- matches: a row for every match
- medals_matches_players: a row for every medal type a player gets in a match
- medals: a row for every medal type

You should write PySpark code to achieve the following:

- disable the default behavior of broadcast joins (`spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")`) (`query_1`)
- join the `medals` and `maps` tables with an explicitly specified a broadcast join (`query_2`)
- join the `match_details`, `matches` and `medal_matches_players` using a bucket join on `match_id` with `16` buckets (`query_3`)
- aggregate the above joined dataframe to answer the following questions:
  - which player has the highest average kills per game? (`query_4a`)
  - which playlist has received the most plays? (`query_4b`)
  - which map was played the most? (`query_4c`)
  - on which map do players receive the highest number of Killing Spree medals? (`query_4d`)
- try at least 3 different versions of partitioned tables, and use `.sortWithinPartitions` to get the smallest footprint possible (hint: playlists and maps are both very low cardinality) (`query_5{a,b,c}`)

Please write your solutions in the corresponding file in the `submissions` folder.
