from chispa.dataframe_comparer import *

from ..src.job_1 import job_1
from collections import namedtuple

GameVertex = namedtuple("GameVertex", "identifier type properties")
Game = namedtuple("Game", "game_id player_name pts season")

def test_vertex_generation(spark):
    input_data = [
        Game(1, "LeBron James", 25, 1996),
        Game(1, "LeBron James", 25, 1996)
    ]

    input_dataframe = spark.createDataFrame(input_data)
    actual_df = job_1(spark, input_dataframe)
    expected_output = [
        GameVertex(
            identifier=1,
            type='Game',
            properties={
                'city': 'San Francisco',
                'pts': '25',
                'season': '1996'
            }
        )
    ]
    expected_df = spark.createDataFrame(expected_output)
    assert_df_equality(actual_df, expected_df, ignore_nullable=True)