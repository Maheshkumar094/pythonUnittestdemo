from pyspark.sql import DataFrame, functions as F

def loanreport():
    spark = SparkSession.builder.master("local[*]").config("spark.port.maxRetries", "1000").config("spark.sql.shuffle.partitions", "1").getOrCreate()
    loan_df=spark.read.option("header",True).cav("../data/loan.csv")
    print(f"LOAN::{loan_df}")
    loan_df.createOrReplace("loan")
    cust_df=spark.read.option("header",True).csv("../data/customer.csv")
    cust_df.createOrReplace("cust")
    print(f"CUST::{cust_df}")
    df=spark.sql("select * from loan join cust on loan.customer_id = cust.customer_id")
    
