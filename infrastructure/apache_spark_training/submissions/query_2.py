from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, broadcast
spark = SparkSession.builder.appName("Jupyter").getOrCreate()

medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")
matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")

#autoBroadcastJoinThreshold 
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
broadcastFromThreshold = medals_matches_players.alias("mmp").join(broadcast(medals).alias("med"), col('mmp.medal_id')==col('med.medal_id')) \
   .select(col("mmp.match_id"), col("med.name"),  col("mmp.player_gamertag")) \
   .join(matches.alias('matches'), col('mmp.match_id')==col('matches.match_id'))\
   .join(broadcast(maps).alias('maps'), col('maps.mapid')==col('matches.mapid'))\
   .explain()

##Output Broadcastjoins on lines 22 and 37.
##== Physical Plan ==
##AdaptiveSparkPlan isFinalPlan=false
##+- BroadcastHashJoin [mapid#565], [mapid#541], Inner, BuildRight, false
##   :- SortMergeJoin [match_id#690], [match_id#564], Inner
##   :  :- Sort [match_id#690 ASC NULLS FIRST], false, 0
##   :  :  +- Exchange hashpartitioning(match_id#690, 200), ENSURE_REQUIREMENTS, [plan_id=658]
##   :  :     +- Project [match_id#690, name#510, player_gamertag#691]
##   :  :        +- BroadcastHashJoin [medal_id#692], [medal_id#500], Inner, BuildRight, false
##   :  :           :- Filter (isnotnull(medal_id#692) AND isnotnull(match_id#690))
##   :  :           :  +- FileScan csv [match_id#690,player_gamertag#691,medal_id#692] Batched: false, DataFilters: [isnotnull(medal_id#692), isnotnull(match_id#690)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/data/medals_matches_players.csv], PartitionFilters: [], PushedFilters: [IsNotNull(medal_id), IsNotNull(match_id)], ReadSchema: struct<match_id:string,player_gamertag:string,medal_id:string>
##   :  :           +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=653]
##   :  :              +- Filter isnotnull(medal_id#500)
##   :  :                 +- FileScan csv [medal_id#500,name#510] Batched: false, DataFilters: [isnotnull(medal_id#500)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/data/medals.csv], PartitionFilters: [], PushedFilters: [IsNotNull(medal_id)], ReadSchema: struct<medal_id:string,name:string>
##   :  +- Sort [match_id#564 ASC NULLS FIRST], false, 0
##   :     +- Exchange hashpartitioning(match_id#564, 200), ENSURE_REQUIREMENTS, [plan_id=659]
##   :        +- Filter (isnotnull(match_id#564) AND isnotnull(mapid#565))
##   :           +- FileScan csv [match_id#564,mapid#565,is_team_game#566,playlist_id#567,game_variant_id#568,is_match_over#569,completion_date#570,match_duration#571,game_mode#572,map_variant_id#573] Batched: false, DataFilters: [isnotnull(match_id#564), isnotnull(mapid#565)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/data/matches.csv], PartitionFilters: [], PushedFilters: [IsNotNull(match_id), IsNotNull(mapid)], ReadSchema: struct<match_id:string,mapid:string,is_team_game:string,playlist_id:string,game_variant_id:string...
##   +- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, false]),false), [plan_id=664]
##      +- Filter isnotnull(mapid#541)
##         +- FileScan csv [mapid#541,name#542,description#543] Batched: false, DataFilters: [isnotnull(mapid#541)], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/iceberg/data/maps.csv], PartitionFilters: [], PushedFilters: [IsNotNull(mapid)], ReadSchema: struct<mapid:string,name:string,description:string>