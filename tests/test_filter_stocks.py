from pyspark.sql import Row
import pytest
from pyspark.sql import functions as F
from stock_analysis.filter_stocks import filter_above_400
from chispa.dataframe_comparer import assert_df_equality

@pytest.fixture(scope="session")
def stock_data(spark):
    data = [
        {"stock": "AA", "price": 200},
        {"stock": "BB", "price": 600},
        {"stock": "CC", "price": 750},
    ]
    df = spark.createDataFrame(Row(**x) for x in data)
    df.cache()
    df.count()
    return df
    
def test_filter_above_400(spark, stock_data):
    expected_data = [
        {"stock": "BB", "price": 600},
        {"stock": "CC", "price": 750},
    ]
    expected_result = spark.createDataFrame(Row(**x) for x in expected_data)
    result = filter_above_400(stock_data)
    assert_df_equality(result, expected_result)