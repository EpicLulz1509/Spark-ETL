import pyspark
from utils.schema import *
from pyspark.sql import SparkSession
### create spark object
spark = SparkSession.builder.appName('Spark12345').getOrCreate()

### file options
file_type = "csv"
first_row_is_header = "True"
delimiter = ","

sales_order_schema = sales_order_schema()

print("Sales order table")

sales_order_df = spark.read.format(file_type).option("sep", delimiter).schema(sales_order_schema).load("srcdata\SalesOrder\SalesOrder.csv")


sales_order_df.toPandas().to_csv('tgtdata\sales_order.csv', index=False)

# print(sales_order_schema)