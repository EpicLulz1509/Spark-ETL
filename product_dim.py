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
product_target_df = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).load("tgtdata\product.csv")

product_dim_category_target_df = product_data_df.select(F.col("Category"), F.col("SKU"), F.col("Subcategory"), F.col("Model")).join(product_target_df, F.col("SKU") == F.col("ItemSku"))
product_dim_category_target_df = product_dim_category_target_df.select(F.col("ProductId"), F.col("Model"), F.col("Category"), F.col("Subcategory"))
product_dim_category_target_df = product_dim_category_target_df.dropDuplicates(['Model', 'Category', 'Subcategory'])
product_dim_category_target_df = product_dim_category_target_df.sort(F.col("ProductId"), ascending=True)

product_dim_category_target_df.toPandas().to_csv('tgtdata\product_dim.csv', index=False) 