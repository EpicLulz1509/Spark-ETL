import pyspark
from utils.schema import *
from pyspark.sql import SparkSession
### create spark object
spark = SparkSession.builder.appName('Spark12345').getOrCreate()


# note1 - customer, 1 note per table