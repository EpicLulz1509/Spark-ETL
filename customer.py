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


def createCustomerTable(path):
    
    cust_schema = customer_schema() 

    location_target_df = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).load("tgtdata/location.csv")
    customer_data_df = spark.read.format(file_type).option("sep", delimiter).option("header", first_row_is_header).schema(cust_schema).load("srcdata/CustomerData/CustomerData.csv")

    customer_target_df = customer_data_df.join(location_target_df,( F.col("PostalCode") == F.col("LocationZip") ) & (F.col("City") == F.col("LocationCity")))
    customer_target_df = customer_target_df.select(F.col("CustomerId"), F.col("CustomerKey"), F.col("LocationId")).orderBy(F.col("CustomerId"))
    customer_target_df.distinct()

    customer_target_df.toPandas().to_csv(path, index=False)
    
    return customer_target_df

# df.write.format_csv
