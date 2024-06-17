import pyspark
from utils.schema import *
from pyspark.sql import SparkSession
### create spark object
spark = SparkSession.builder.appName('Spark12345').getOrCreate()

### file options
file_type = "csv"
first_row_is_header = "True"
delimiter = ","

sales_schema = sales_schema()

print("Sales table")

sales_df = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).schema(sales_schema).load("srcdata\Sales\Sales.csv")


sales_df.toPandas().to_csv('tgtdata\sales.csv', index=False)