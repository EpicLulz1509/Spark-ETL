import pyspark
from utils.schema import *
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *

### create spark object
spark = SparkSession.builder.appName('Spark12345').getOrCreate()

### file options
file_type = "csv"
first_row_is_header = "True"
delimiter = ","

product_schema = product_schema()

product_data_df = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).schema(product_schema).load("srcdata\ProductData\ProductData.csv")

item_target_df = product_data_df.select(F.col("SKU").alias("ItemSku"), F.col("StandardCost"), F.col("ListPrice"))
item_target_df = item_target_df.distinct()

item_target_df.toPandas().to_csv('tgtdata\item.csv', index=False)
