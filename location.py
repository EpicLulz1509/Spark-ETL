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

cust_schema = customer_schema()
sales_territory_schema = sales_territory_schema()

customer_data_df = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).schema(cust_schema).load("srcdata\CustomerData\CustomerData.csv")
sales_territory_df = spark.read.format(file_type).option("sep", delimiter).schema(sales_territory_schema).load("srcdata\SalesTerritory\SalesTerritory.csv")
country_target_df = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).load("tgtdata\country.csv")

location_target_df = customer_data_df.join(sales_territory_df, F.col("CountryRegion") == F.col("Country")).select(F.col("City"), F.col("StateProvince"), F.col("Country"), F.col("PostalCode"))
location_target_df = location_target_df.distinct()
location_target_df = location_target_df.join(country_target_df, F.col("Country") == F.col("CountryName"))
location_target_df = location_target_df.select((monotonically_increasing_id()+1).alias("LocationId"), F.col("City"), F.col("StateProvince"), F.col("CountryId"), F.col("PostalCode")) 
location_target_df = location_target_df.select(F.col("LocationId"), F.col("City").alias("LocationCity"), F.col("StateProvince").alias("StateProvince"), F.col("CountryId"), F.col("PostalCode").alias("LocationZip"))

location_target_df.toPandas().to_csv('tgtdata\location.csv', index=False)
