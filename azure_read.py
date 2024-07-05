from azure.storage.blob import ContainerClient
from io import StringIO
import pandas as pd

conn_str = "DefaultEndpointsProtocol=https;AccountName=basic123;AccountKey=5HI0CB8oYQ2oigiJ/vE/HtDu76sNMqyiXRy0FYITJQlOn3BJQKfiLCbAHYRxBEtOtocYqpbAKJc0+ASt0gquWw==;EndpointSuffix=core.windows.net"
container_name = "target-tables"
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
