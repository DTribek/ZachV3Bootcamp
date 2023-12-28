from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, broadcast, year
spark = SparkSession.builder.appName("Jupyter").getOrCreate()

matches = spark.sql("SELECT *FROM diegotribek.bucketed_matches")
match_details = spark.sql("SELECT *FROM diegotribek.bucketed_match_details")
medals_matches_players = spark.sql("SELECT *FROM diegotribek.bucketed_medals_matches_players")

joined_df =  matches.alias("m").join(match_details.alias("md"), col("m.match_id") == col("md.match_id")).join(medals_matches_players.alias("mmp"), col("m.match_id") == col("mmp.match_id"))

kills = joined_df.select('md.player_gamertag', 'player_total_kills')\
                  .groupBy('md.player_gamertag')\
                  .agg({'player_total_kills': 'avg'})\
                  .orderBy(col("avg(player_total_kills)").desc()).limit(1)
kills.show()

##Output
#player_gamertag: gimpinator14
#avg(player_total_kills): 109.0
