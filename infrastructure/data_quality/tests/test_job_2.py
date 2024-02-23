from chispa.dataframe_comparer import *

from ..src.job_2 import job_2
from collections import namedtuple

ActorSeason = namedtuple("ActorSeason", "actor actor_id quality_class is_active current_year")
ActorScd = namedtuple("ActorScd", "actor actor_id quality_class is_active start_date end_date")
    
def test_vertex_generation(spark):
    input_data = [
        ActorSeason("Zezinho", 1, "Average", True, 1951),
        ActorSeason("Zezinho", 1, "Average", False, 1952)
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = job_2(spark, input_dataframe)
    
    expected_output = [
        ActorScd("Zezinho", 1, "Average", True, 1950, 1951),
        ActorScd("Zezinho", 1, "Average", False, 1951, 1952)
    ]
    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)
    