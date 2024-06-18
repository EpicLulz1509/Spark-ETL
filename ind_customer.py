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

customer_data_df = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).schema(cust_schema).load("srcdata\CustomerData\CustomerData.csv")

individual_customer_target_df = customer_data_df.select(F.col("CustomerId"), F.col("Customer"))
individual_customer_target_df.distinct()

individual_customer_target_df.toPandas().to_csv('tgtdata\ind_customer.csv', index=False)

# df.write.format_csv
