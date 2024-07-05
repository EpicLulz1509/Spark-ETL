from azure.storage.blob import BlobServiceClient
import pandas as pd
import io
import os

# Define your Azure Blob Storage account details
account_name = os.environ["ACCOUNT_NAME"]
account_key = os.environ["ACCOUNT_KEY"]
container_name = os.environ["CONTAINER_NAME"]
# blob_name = 'newvirtual.csv'
df = pd.DataFrame({'col1': [1, 2], 'col2': [3, 4]})

def save_blob(targetdf, blob_name):

    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
    
    print("Target table: ")
    
    print(targetdf)
    print(type(targetdf))   
    
    print("Azure blob: ")
    
    print(blob_name)
    print(type(blob_name))
    
    # Convert your pandas dataframe to a CSV string
    csv_string = targetdf.toPandas().to_csv(index=False)

    # Convert the CSV string to a bytes object
    csv_bytes = str.encode(csv_string)

    # Upload the bytes object to Azure Blob Storage
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(csv_bytes, overwrite=True)
