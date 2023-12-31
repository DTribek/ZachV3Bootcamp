from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, broadcast
spark = SparkSession.builder.appName("Jupyter").getOrCreate()

medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")
matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")

spark.sql("""DROP TABLE IF EXISTS diegotribek.bucketed_medals_matches_players""")
bucketedDDL = """
 CREATE TABLE IF NOT EXISTS diegotribek.bucketed_medals_matches_players (
     match_id STRING,
     player_gamertag STRING,
     medal_id BIGINT,
     count INT
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """
spark.sql(bucketedDDL)

medals_matches_players.select(
     col("match_id"), col("player_gamertag"), col("medal_id"), col("count")
     ) \
     .write.mode("append")  \
     .bucketBy(16, "match_id").saveAsTable("diegotribek.bucketed_medals_matches_players")

spark.sql("""DROP TABLE IF EXISTS diegotribek.bucketed_match_details""")
bucketedDetailsDDL = """
 CREATE TABLE IF NOT EXISTS diegotribek.bucketed_match_details (
     match_id STRING,
     player_gamertag STRING,
     player_total_kills INTEGER
 )
 USING iceberg
 PARTITIONED BY (bucket(16, match_id));
 """
spark.sql(bucketedDetailsDDL)

match_details.select(
    col('match_id'), col('player_gamertag'), col('player_total_kills')
    ) \
    .write.mode("append")  \
    .bucketBy(16, "match_id").saveAsTable("diegotribek.bucketed_match_details")

spark.sql("""DROP TABLE IF EXISTS diegotribek.bucketed_matches""")
bucketedDDL = """
 CREATE TABLE IF NOT EXISTS diegotribek.bucketed_matches (
     match_id STRING,
     mapid STRING,
     is_team_game BOOLEAN,
     playlist_id STRING,
     completion_date INT
 )
 USING iceberg
 PARTITIONED BY (completion_date, bucket(16, match_id));
 """
spark.sql(bucketedDDL)

matches = matches.withColumn("completion_date", year('completion_date'))
matches.select(
     col("match_id"), col("mapid"), col("is_team_game"), col("playlist_id"), col("completion_date")
     ) \
     .write.mode("append")  \
     .partitionBy("completion_date") \
     .bucketBy(16, "match_id").saveAsTable("diegotribek.bucketed_matches")


spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
spark.sql("""
    SELECT * FROM 
    
    diegotribek.bucketed_matches m
    
    INNER JOIN diegotribek.bucketed_match_details md
    ON m.match_id = md.match_id

    INNER JOIN diegotribek.bucketed_medals_matches_players mmp
    ON m.match_id = mmp.match_id
    
    LIMIT 10000
""").explain()

#
#== Physical Plan ==
#AdaptiveSparkPlan isFinalPlan=false
#+- CollectLimit 10000
#   +- SortMergeJoin [match_id#1619], [match_id#1627], Inner
#      :- SortMergeJoin [match_id#1619], [match_id#1624], Inner
#      :  :- Sort [match_id#1619 ASC NULLS FIRST], false, 0
#      :  :  +- Exchange hashpartitioning(match_id#1619, 200), ENSURE_REQUIREMENTS, [plan_id=797]
#      :  :     +- BatchScan demo.diegotribek.bucketed_matches[match_id#1619, mapid#1620, is_team_game#1621, playlist_id#1622, completion_date#1623] demo.diegotribek.bucketed_matches (branch=null) [filters=match_id IS NOT NULL, groupedBy=] RuntimeFilters: []
#      :  +- Sort [match_id#1624 ASC NULLS FIRST], false, 0
#      :     +- Exchange hashpartitioning(match_id#1624, 200), ENSURE_REQUIREMENTS, [plan_id=798]
#      :        +- BatchScan demo.diegotribek.bucketed_match_details[match_id#1624, player_gamertag#1625, player_total_kills#1626] demo.diegotribek.bucketed_match_details (branch=null) [filters=match_id IS NOT NULL, groupedBy=] RuntimeFilters: []
#      +- Sort [match_id#1627 ASC NULLS FIRST], false, 0
#         +- Exchange hashpartitioning(match_id#1627, 200), ENSURE_REQUIREMENTS, [plan_id=804]