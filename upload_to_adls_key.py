import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient

# Load environment variables from .env file
load_dotenv()

account_name = os.getenv("AZURE_ACCOUNT_NAME")
account_key = os.getenv("AZURE_ACCOUNT_KEY")
container_name = os.getenv("AZURE_CONTAINER_NAME")
local_folder = "data"

# Build connection string
connection_string = f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key};EndpointSuffix=core.windows.net"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)

# Upload JSON files
for filename in os.listdir(local_folder):
    if filename.endswith(".json"):
        file_path = os.path.join(local_folder, filename)
        blob_client = container_client.get_blob_client(blob=filename)
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
            print(f"✅ Uploaded {filename} to ADLS.")
