import streamlit as st
import pandas as pd
import altair as alt
# %matplotlib inline    
import plotly.express as px
from azure_read import *
from paths import *
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql import functions as F
from pyspark.sql.functions import *
import pyspark.pandas as ps

### create spark object
spark = SparkSession.builder.appName('Spark12345').getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

### file options
file_type = "csv"
first_row_is_header = "True"
delimiter = ","

st.set_page_config(
    page_title="Adventure Works Dashboard",
    layout="wide",
    initial_sidebar_state="expanded")

alt.themes.enable("dark")
alt.renderers.enable("mimetype")

df = pd.read_csv('tgtdata\customer.csv')

with st.sidebar:
    st.title('Customer Locations')
    
def customer_location(customer_df, location_df, country_df):
    
    customer_df = spark.createDataFrame(customer_df)
    location_df = spark.createDataFrame(location_df)
    country_df = spark.createDataFrame(country_df)

    new_df = customer_df.join(location_df, (customer_df['LocationId'] == location_df['LocationId'])).join(country_df, (country_df['CountryId'] == location_df['CountryId']))
    new_df = new_df.select(F.col("CustomerId"), F.col("CountryName"))
    new_df = new_df.toPandas()
    
    cust_group_by_location = new_df.groupby('CountryName', as_index=False)['CustomerId'].count()
    
    choropleth = px.choropleth(cust_group_by_location, locations='CountryName', color='CustomerId', locationmode="country names",
                            #    color_continuous_scale=input_color_theme,
                               range_color=(0, 18839),
                               labels={'CountryName':'Country',
                                       'CustomerId' : 'Customer Count'},
                               title="Count of customers per country"
                              )
    choropleth.update_layout(margin=dict(l=10, r=10, t=25, b=10))
    return choropleth


def render_graphs():    
    col = st.columns((1), gap='medium')
    customer_df = read_data(path_customer)
    location_df = read_data(path_location)
    country_df = read_data(path_country)
    
    with col[0]:
        plot = customer_location(customer_df, location_df, country_df)
        # st.altair_chart(plot)
        st.plotly_chart(plot)

render_graphs()

    
