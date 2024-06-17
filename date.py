import pyspark
from utils.schema import *
from pyspark.sql import SparkSession
### create spark object
spark = SparkSession.builder.appName('Spark12345').getOrCreate()

### file options
file_type = "csv"
first_row_is_header = "True"
delimiter = ","

date_schema = date_schema()

print("Date table")

sales_df = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).schema(date_schema).load("srcdata\Date\Date.csv")


sales_df.toPandas().to_csv('tgtdata\date.csv', index=False)