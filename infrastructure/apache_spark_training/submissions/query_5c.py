from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, broadcast, year, countDistinct
spark = SparkSession.builder.appName("Jupyter").getOrCreate()

medals = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals.csv")
maps = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/maps.csv")
matches = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/matches.csv")
match_details = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/match_details.csv")
medals_matches_players = spark.read.option("header", "true").option("inferSchema", "true").csv("/home/iceberg/data/medals_matches_players.csv")
matches.createOrReplaceTempView("matches")
match_details.createOrReplaceTempView("match_details")
medals_matches_players.createOrReplaceTempView("medals_matches_players")

joined_df =  matches.alias("m")\
            .join(match_details.alias("md"), col("m.match_id") == col("md.match_id"))\
            .join(medals_matches_players.alias("mmp"), col("m.match_id") == col("mmp.match_id"))\
            .select("m.match_id"\
                    ,"m.mapid"\
                    ,"m.is_team_game"\
                    ,"m.playlist_id"\
                    ,"m.completion_date"\
                    ,"md.player_gamertag"\
                    ,"md.player_total_kills"\
                    ,"mmp.medal_id"\
                    ,"mmp.count")


joined_df3 = joined_df.repartition(32, col('mapid')).sortWithinPartitions(col('mapid'), col('playlist_id'))
joined_df3.write.mode("overwrite").saveAsTable('diegotribek.sort_3')


#%%sql
#SELECT SUM(file_size_in_bytes) as size, count(1) as num_files, 'sort_1'
#FROM diegotribek.sort_1.files

#UNION ALL
#SELECT SUM(file_size_in_bytes) as size, count(1) as num_files, 'sort_2'
#FROM diegotribek.sort_2.files

#UNION ALL
#SELECT SUM(file_size_in_bytes) as size, count(1) as num_files, 'sort_3'
#FROM diegotribek.sort_3.files


#size	    num_files	sort_1
#10600908	7	        sort_1
#10212783	15	        sort_2
#10410617	14	        sort_3