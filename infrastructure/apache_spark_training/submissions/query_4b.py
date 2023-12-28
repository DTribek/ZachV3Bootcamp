from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, col, broadcast, year, countDistinct
spark = SparkSession.builder.appName("Jupyter").getOrCreate()

playlist = joined_df.select('m.match_id', 'playlist_id')\
                  .groupBy('playlist_id')\
                  .agg(countDistinct("m.match_id"))\
                  .orderBy(col("count(DISTINCT match_id)").desc()).limit(1)
playlist.show(truncate=0)

#Output
#playlist_id: |f72e0ef0-7c4a-4307-af78-8e38dac3fdba 
#count(DISTINCT match_id)|:7640

