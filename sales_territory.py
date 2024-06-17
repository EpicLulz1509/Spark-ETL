import pyspark
from utils.schema import *
from pyspark.sql import SparkSession
### create spark object
spark = SparkSession.builder.appName('Spark12345').getOrCreate()

### file options
file_type = "csv"
first_row_is_header = "True"
delimiter = ","

sales_territory_schema = sales_territory_schema()

print("Sales Territory table")

sales_territory_df = spark.read.format(file_type).option("sep", delimiter).schema(sales_territory_schema).load("srcdata\SalesTerritory\SalesTerritory.csv")


sales_territory_df.toPandas().to_csv('tgtdata\sales_territory.csv', index=False)