from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col
spark = SparkSession.builder.appName("Jupyter").getOrCreate()

medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")
matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
noBroadcast = matches.alias("m").join(maps.alias("map"), col("m.mapid") == col("map.mapid")) \
   .select(col("m.completion_date"), col("map.name"),  col("m.game_mode")) \
   .explain()

#Output SortedMergeJoin on line 20 instead of BroadcastHashJoin 
#== Physical Plan ==
#AdaptiveSparkPlan isFinalPlan=false
#+- Project [completion_date#570, name#542, game_mode#572]
#   +- SortMergeJoin [mapid#565], [mapid#541], Inner
#      :- Sort [mapid#565 ASC NULLS FIRST], false, 0
#      :  +- Exchange hashpartitioning(mapid#565, 200), ENSURE_REQUIREMENTS, [plan_id=504]
#      :     +- Filter isnotnull(mapid#565)
#      :        +- FileScan csv [mapid#565,completion_date#570,game_mode#572] Batched: false, DataFilters: [isnotnull(mapid#565)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/data/matches.csv], PartitionFilters: [], PushedFilters: [IsNotNull(mapid)], ReadSchema: struct<mapid:string,completion_date:string,game_mode:string>
#      +- Sort [mapid#541 ASC NULLS FIRST], false, 0
#         +- Exchange hashpartitioning(mapid#541, 200), ENSURE_REQUIREMENTS, [plan_id=505]
#            +- Filter isnotnull(mapid#541)
#               +- FileScan csv [mapid#541,name#542] Batched: false, DataFilters: [isnotnull(mapid#541)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/data/maps.csv], PartitionFilters: [], PushedFilters: [IsNotNull(mapid)], ReadSchema: struct<mapid:string,name:string>
