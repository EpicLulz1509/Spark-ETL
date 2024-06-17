import pyspark
from utils.schema import *
from pyspark.sql import SparkSession
### create spark object
spark = SparkSession.builder.appName('Spark12345').getOrCreate()

### file options
file_type = "csv"
first_row_is_header = "True"
delimiter = ","

product_schema = product_schema()

print("Product table")

product_data_df = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).schema(product_schema).load("srcdata\ProductData\ProductData.csv")


product_data_df.toPandas().to_csv('tgtdata\product.csv', index=False)