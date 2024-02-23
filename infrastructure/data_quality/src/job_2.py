from typing import Optional
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

def query_2(output_table_name: str) -> str:
    return f"""
    WITH actors_lagged as (
        SELECT 
            actor,
            actor_id,
            quality_class,
            is_active,
            LAG(is_active, 1) OVER (PARTITION BY actor_id ORDER BY current_year) as is_active_last_year,
            LAG(quality_class, 1) OVER (PARTITION BY actor_id ORDER BY current_year) as quality_class_last_year,
            current_year
        FROM diegotribek.actors
        WHERE current_year <= 1919
    ),

    /*Get the changes in "is_active" and "quality_class" column*/
    streaked as (
    SELECT *,
        SUM(CASE WHEN is_active = is_active_last_year AND quality_class = quality_class_last_year THEN 0 ELSE 1 END) OVER (PARTITION BY actor_id ORDER BY current_year) as streak_identifier
    FROM actors_lagged 
    )

    /*Pull all information together, grouping by the actor and the changes identified in
    the previous table*/
    SELECT 
        actor,
        actor_id,
        MAX(quality_class) as quality_class,
        MAX(is_active) as is_active,
        MIN(current_year) as start_date,
        MAX(current_year) as end_date,
        1919 as current_year --Fixed value (considered in where clause, first table)
        FROM streaked 
    GROUP BY  
        actor,
        actor_id,
        streak_identifier
    """

def job_2(spark_session: SparkSession, output_table_name: str) -> Optional[DataFrame]:
  output_df = spark_session.table(output_table_name)
  output_df.createOrReplaceTempView(output_table_name)
  return spark_session.sql(query_2(output_table_name))

def main():
    output_table_name: str = "actors_scd"
    spark_session: SparkSession = (
        SparkSession.builder
        .master("local")
        .appName("job_2")
        .getOrCreate()
    )
    output_df = job_2(spark_session, output_table_name)
    output_df.write.mode("overwrite").insertInto(output_table_name)