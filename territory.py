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

sales_territory_schema = sales_territory_schema()

sales_territory_df = spark.read.format(file_type).option("sep", delimiter).schema(sales_territory_schema).load("srcdata\SalesTerritory\SalesTerritory.csv")
product_target_df = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).load("tgtdata\product.csv")

territory_target_df = sales_territory_df.select(F.col("Region"), F.col("Country"),  F.col("Group"), F.col("SalesTerritoryKey")).distinct()
territory_target_df = territory_target_df.select(F.col("SalesTerritoryKey").alias("TerritoryId"), F.col("Group"), F.col("Country"), F.col("Region")).orderBy("TerritoryId")

territory_target_df.toPandas().to_csv('tgtdata\\territory.csv', index=False) 