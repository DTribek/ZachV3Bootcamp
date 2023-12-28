from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, broadcast, year, countDistinct
spark = SparkSession.builder.appName("Jupyter").getOrCreate()

maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")
matches = spark.sql("SELECT *FROM diegotribek.bucketed_matches")
match_details = spark.sql("SELECT *FROM diegotribek.bucketed_match_details")
medals_matches_players = spark.sql("SELECT *FROM diegotribek.bucketed_medals_matches_players")

joined_df =  matches.alias("m").join(match_details.alias("md"), col("m.match_id") == col("md.match_id")).join(medals_matches_players.alias("mmp"), col("m.match_id") == col("mmp.match_id"))

maps_plays = joined_df.select('m.match_id', 'mapid')\
                  .groupBy('mapid')\
                  .agg(countDistinct("m.match_id"))\
                  .orderBy(col("count(DISTINCT match_id)").desc()).limit(1)

maps_plays.alias('mp').join(maps.alias('m'), col('mp.mapid') == col('m.mapid')).select('mp.mapid','count(DISTINCT match_id)','name').show(truncate=0)

#Output
#mapid: c7edbf0f-f206-11e4-aa52-24be05e24f7e
#count(DISTINCT match_id): 7032
#name:Breakout Arena