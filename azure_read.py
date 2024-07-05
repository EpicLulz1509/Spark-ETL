from azure.storage.blob import ContainerClient
from io import StringIO
import pandas as pd
import os 

conn_str = os.environ["CONN_STR"]
container_name = os.environ["CONTAINER_NAME"]
blob_name = "customer.csv"

# Define your Azure Blob Storage account details
account_name = 'basic123'


# Create a ContainerClient instance via connection string auth.
container_client = ContainerClient.from_connection_string(conn_str, container_name)


def read_data(blob_name):
    # Download blob as StorageStreamDownloader object (stored in memory)
    downloaded_blob = container_client.download_blob(blob_name, encoding='utf8')
    df = pd.read_csv(StringIO(downloaded_blob.readall()), low_memory=False)
    return df

# print(df.head(5))
