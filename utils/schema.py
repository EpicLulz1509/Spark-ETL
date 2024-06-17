from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

def customer_schema():
    customer_data_schema = StructType([
    StructField('CustomerKey',          IntegerType(), nullable=True),
    StructField('CustomerId',        StringType(), nullable=True),
    StructField('Customer', StringType(), nullable=True),
    StructField('City', StringType(), nullable=True),
    StructField('StateProvince', StringType(), nullable=True),
    StructField('CountryRegion', StringType(), nullable=True),
    StructField('PostalCode',      StringType(), nullable=True)])
    
    return(customer_data_schema)
    