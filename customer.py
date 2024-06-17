import pyspark
from utils.schema import *
from pyspark.sql import SparkSession
### create spark object
spark = SparkSession.builder.appName('Spark12345').getOrCreate()

### file options
file_type = "csv"
first_row_is_header = "True"
delimiter = ","

cust_schema = customer_schema()

print("Customer table")

customer_data_df = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).schema(cust_schema).load("srcdata\CustomerData\CustomerData.csv")

customer_data_df.toPandas().to_csv('tgtdata\customer.csv', index=False)

print("Customer table written")
# df.write.format_csv
