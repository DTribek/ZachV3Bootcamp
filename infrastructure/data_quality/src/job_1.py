from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_1(output_table_name: str) -> str:
    return f"""
  WITH dedup_nba_games as (
    SELECT 
        *,
        ROW_NUMBER() OVER(PARTITION BY game_id ORDER BY game_date_est DESC) as row_num
    FROM bootcamp.nba_games
  )

  SELECT 
    game_id AS identifier,
    game as `type`,
    map(
        'player_name', player_name
        'pts', CAST(pts AS STRING),
        'season', CAST(season AS STRING)
    ) as properties
  FROM
  dedup_nba_games 
  WHERE row_num = 1
    """

def job_1(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_1(output_table_name))

def main():
    output_table_name: str = "game_deduped"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_1")
        .getOrCreate()
    )
    output_df = job_1(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)