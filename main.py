import pyspark
from utils.schema import *
from customer import *
from ind_customer import *
from product import *
from product_dim import *
from azure_save import *
from pyspark.sql import SparkSession
import pandas as pd
### create spark object
spark = SparkSession.builder.appName('Spark12345').getOrCreate()

local = "tgtdata"

path_customer = 'Customer.csv'
path_ind_customer = 'IndCustomer.csv'
path_product = 'Product.csv'
path_product_dim = 'ProductDim.csv'

def createTablesSet1():
    customer_df = createCustomerTable(f"{local}\{path_customer}")
    ind_customer_df = createIndCustomerTable(f"{local}\{path_ind_customer}")
    product_df = createProductTable(f"{local}\{path_product}")
    product_dim_df = createProductDimTable(f"{local}\{path_product_dim}")
    
    save_blob(customer_df, f"{path_customer}")
    save_blob(ind_customer_df, f"{path_ind_customer}")
    save_blob(product_df, f"{path_product}")
    save_blob(product_dim_df, f"{path_product_dim}")

createTablesSet1()
