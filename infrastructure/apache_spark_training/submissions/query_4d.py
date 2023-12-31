from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, broadcast, year, countDistinct, count
spark = SparkSession.builder.appName("Jupyter").getOrCreate()


maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")
matches = spark.sql("SELECT *FROM diegotribek.bucketed_matches")
match_details = spark.sql("SELECT *FROM diegotribek.bucketed_match_details")
medals_matches_players = spark.sql("SELECT *FROM diegotribek.bucketed_medals_matches_players")



medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
killing_spree_medals = medals.filter((medals.name == "Killing Spree"))

joined_df =  matches.alias("m")\
            .join(match_details.alias("md"), col("m.match_id") == col("md.match_id"))\
            .join(medals_matches_players.alias("mmp"), col("m.match_id") == col("mmp.match_id"))\
            .join(killing_spree_medals.alias('k'), col("mmp.medal_id")==col("k.medal_id"), how="inner")\
            .select("m.match_id"\
                    ,"m.mapid"\
                    ,"m.is_team_game"\
                    ,"m.playlist_id"\
                    ,"m.completion_date"\
                    ,"md.player_gamertag"\
                    ,"md.player_total_kills"\
                    ,"mmp.medal_id"\
                    ,"mmp.count")

medals = joined_df.select('mapid', 'medal_id')\
                  .groupBy('mapid')\
                  .agg(count("medal_id"))\
                  .orderBy(col("count(medal_id)").desc()).limit(1)

named_map = medals.alias('me').join(maps.alias('m'), col('m.mapid') == col('me.mapid')).select('m.mapid','count(medal_id)','name')
named_map.show(truncate=0)

#Output
#mapid: c74c9d0f-f206-11e4-8330-24be05e24f7e
#count(medal_id): 56908           
#name:Alpine