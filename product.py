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

def createProductTable(path):
    
    # path = 'tgtdata\product.csv'

    product_schema1 = product_schema()

    product_data_df = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).schema(product_schema1).load("srcdata\ProductData\ProductData.csv")
    item_target_df = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).load("tgtdata\item.csv")

    product_target_df = product_data_df.select(F.col("Product"), F.col("SKU"), F.col("Color")).join(item_target_df, F.col("SKU") == F.col("ItemSku"))
    product_target_df = product_target_df.select(F.col("Product").alias("ProductName"), F.col("SKU").alias("ItemSku"), F.col("Color").alias("ProductColor"))
    product_target_df  = product_target_df.distinct()
    product_target_df = product_target_df.select((monotonically_increasing_id()+1).alias("ProductId"), F.col("ProductName"), F.col("ItemSku"), F.col("ProductColor"))
    product_target_df = product_target_df.orderBy("ProductId")

    product_target_df.toPandas().to_csv(path, index=False)
    
    return product_target_df
    