from pyspark.sql import DataFrame, functions as F
from pyspark.sql import SparkSession

def loanreport():
    spark = SparkSession.builder.master("local[*]").config("spark.port.maxRetries", "1000").config("spark.sql.shuffle.partitions", "1").getOrCreate()
    loan_df=spark.read.option("header",True).csv("../pythondemo/data/loan.csv")
    
    loan_df.createOrReplaceTempView("loan")
    loan_df.show()
    cust_df=spark.read.option("header",True).csv("../pythondemo/data/customer.csv")
    cust_df.createOrReplaceTempView("cust")
    cust_df.show()
    df=spark.sql("select * from loan join cust on loan.customer_id = cust.customer_id")
    print(f"count :: {df.count()}")
    df.show()


if __name__ == "__main__":
    loanreport()
