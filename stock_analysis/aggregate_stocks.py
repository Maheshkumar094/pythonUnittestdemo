from pyspark.sql import DataFrame, functions as F

def get_max_stock_price(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("stock").agg(F.max("price").alias("max_price"))
    )




def get_min_stock_price(df: DataFrame) -> DataFrame:
    return (
        df
        .groupBy("stock")
        .agg(
            F.min("price").alias("min_price")
        )
    )