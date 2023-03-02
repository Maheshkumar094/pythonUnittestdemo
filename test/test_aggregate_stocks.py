from pyspark.sql import Row
import pytest
from pyspark.sql import functions as F
from stock_analysis.aggregate_stocks import *
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql.types import *

@pytest.fixture(scope="session")
def stock_data(spark):
    schema = StructType([
        StructField("stock", StringType()),
        StructField("date", StringType()),
        StructField("price", DoubleType())
    ])
    data = [
        {"stock": "AA", "date": "20180902", "price": 200.0},
        {"stock": "AA", "date": "20181002", "price": 300.50},
        {"stock": "AA", "date": "20181102", "price": 400.50},
        {"stock": "BB", "date": "20180902", "price": None},
        {"stock": "BB", "date": "20181002", "price": 450.50},
        {"stock": "BB", "date": "20181102", "price": 200.50},
    ]
    df = spark.createDataFrame([Row(**x) for x in data], schema)
    df.cache()
    df.count()
    return df
    
def test_get_max_stock_price(spark, stock_data):
    print("Calculating max staock value")
    schema = StructType([
        StructField("stock", StringType()),
        StructField("max_price", DoubleType())
    ])
    expected_data = [
        {"stock": "AA", "max_price": 400.50},
        {"stock": "BB", "max_price": 450.50},
    ]
    expected_result = spark.createDataFrame(
        [Row(**x) for x in expected_data], 
        schema
    )
    result = get_max_stock_price(stock_data)
    assert_df_equality(
        result, 
        expected_result, 
        ignore_column_order=True, 
        ignore_row_order=True
    )
    
def test_get_min_stock_price(spark, stock_data):
    print("Calculating min staock value")
    schema = StructType([
        StructField("stock", StringType()),
        StructField("min_price", DoubleType())
    ])
    expected_data = [
        {"stock": "AA", "min_price": 200.00},
        {"stock": "BB", "min_price": 200.50},
    ]
    expected_result = spark.createDataFrame(
        [Row(**x) for x in expected_data], 
        schema
    )
    result = get_min_stock_price(stock_data)
    assert_df_equality(
        result,
        expected_result, 
        ignore_column_order=True, 
        ignore_row_order=True
    )

