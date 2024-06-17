from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

def customer_schema():
    customer_schema = StructType([
    StructField('CustomerKey',          IntegerType(), nullable=True),
    StructField('CustomerId',        StringType(), nullable=True),
    StructField('Customer', StringType(), nullable=True),
    StructField('City', StringType(), nullable=True),
    StructField('StateProvince', StringType(), nullable=True),
    StructField('CountryRegion', StringType(), nullable=True),
    StructField('PostalCode',      StringType(), nullable=True)])
    return(customer_schema)


def sales_order_schema():
    sales_order_schema = StructType([
    StructField('SalesOrderLine',          StringType(), nullable=True),
    StructField('SalesOrder',        StringType(), nullable=True),
    StructField('SalesOrderLineKey', IntegerType(), nullable=True),
    StructField('Channel',      StringType(), nullable=True)])  
    return(sales_order_schema)

def sales_schema():
    sales_schema = StructType([
    StructField('SalesAmount',          FloatType(), nullable=True),
    StructField('TotalProductCost',        FloatType(), nullable=True),
    StructField('ProductStandardCost', FloatType(), nullable=True),
    StructField('UnitPriceDiscountPct', FloatType(), nullable=True),
    StructField('ExtendedAmount', FloatType(), nullable=True),
    StructField('UnitPrice', FloatType(), nullable=True),
    StructField('OrderQuantity', IntegerType(), nullable=True),
    StructField('SalesTerritoryKey', IntegerType(), nullable=True),
    StructField('ShipDateKey', IntegerType(), nullable=True),
    StructField('DueDateKey', IntegerType(), nullable=True),
    StructField('OrderDateKey', IntegerType(), nullable=True),
    StructField('ProductKey', IntegerType(), nullable=True),
    StructField('CustomerKey', IntegerType(), nullable=True),
    StructField('ResellerKey', IntegerType(), nullable=True),
    StructField('SalesOrderLineKey',      IntegerType(), nullable=True)]) 
    return(sales_schema)

def date_schema():
    date_schema = StructType([
    StructField('DateKey',          IntegerType(), nullable=True),
    StructField('Date',        StringType(), nullable=True),
    StructField('FiscalYear', StringType(), nullable=True),
    StructField('FiscalQuarter', StringType(), nullable=True),
    StructField('Month', StringType(), nullable=True),
    StructField('FullDate', StringType(), nullable=True),
    StructField('MonthKey',      StringType(), nullable=True)])
    return(date_schema)

def product_schema():
    product_schema = StructType([
    StructField('Category',          StringType(), nullable=True),
    StructField('Subcategory',        StringType(), nullable=True),
    StructField('Model', StringType(), nullable=True),
    StructField('ListPrice', FloatType(), nullable=True),
    StructField('Color', StringType(), nullable=True),
    StructField('StandardCost', FloatType(), nullable=True),
    StructField('Product', StringType(), nullable=True),
    StructField('SKU', StringType(), nullable=True),
    StructField('ProductKey',      IntegerType(), nullable=True)])
    return(product_schema)

def sales_territory_schema():
    sales_territory_schema = StructType([
    StructField('SalesTerritoryKey',          IntegerType(), nullable=True),
    StructField('Region',        StringType(), nullable=True),
    StructField('Country', StringType(), nullable=True),
    StructField('Group',      StringType(), nullable=True)])
    return(sales_territory_schema)

def reseller_schema():
    reseller_schema = StructType([
    StructField('PostalCode',          StringType(), nullable=True),
    StructField('CountryRegion',        StringType(), nullable=True),
    StructField('StateProvince', StringType(), nullable=True),
    StructField('City', StringType(), nullable=True),
    StructField('Reseller', StringType(), nullable=True),
    StructField('BusinessType', StringType(), nullable=True),
    StructField('ResellerId', StringType(), nullable=True),
    StructField('ResellerKey',      IntegerType(), nullable=True)])
    return(reseller_schema)
    