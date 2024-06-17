import pyspark
from utils.schema import *
from pyspark.sql import SparkSession
### create spark object
spark = SparkSession.builder.appName('Spark12345').getOrCreate()

### file options
file_type = "csv"
first_row_is_header = "True"
delimiter = ","

reseller_schema = reseller_schema()

print("Reseller table")

reseller = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).schema(reseller_schema).load("srcdata\Reseller\Reseller.csv")


reseller.toPandas().to_csv('tgtdata\\reseller.csv', index=False)