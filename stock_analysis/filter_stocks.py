from pyspark.sql import DataFrame, functions as F

def filter_above_400(df: DataFrame):
    return df.filter(F.col("price") > 400)

