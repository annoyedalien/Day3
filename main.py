from azure.identity import DefaultAzureCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.mgmt.resource.resources.models import ResourceGroup
from azure.mgmt.storage.models import StorageAccountCreateParameters, Sku, Kind, StorageAccountUpdateParameters, StorageAccountCheckNameAvailabilityParameters
from azure.storage.blob import BlobServiceClient, PublicAccess
from dotenv import load_dotenv
import time
import os
import subprocess

# Load environment variables from .env file
load_dotenv()

# Retrieve variables from environment
subscription_id = os.getenv('AZURE_SUBSCRIPTION_ID')
resource_group_name = os.getenv('RESOURCE_GROUP_NAME')
storage_account_name = os.getenv('STORAGE_ACCOUNT_NAME')
location = os.getenv('LOCATION')
container_name = os.getenv('CONTAINER_NAME')

# Authenticate using DefaultAzureCredential
credential = DefaultAzureCredential()

# Create Resource Management Client
resource_client = ResourceManagementClient(credential, subscription_id)

# Check if the resource group exists
resource_group_exists = resource_client.resource_groups.check_existence(resource_group_name)
if not resource_group_exists:
    resource_group_params = {'location': location}
    resource_client.resource_groups.create_or_update(resource_group_name, ResourceGroup(**resource_group_params))
    print(f"Resource group '{resource_group_name}' created.")
else:
    print(f"Resource group '{resource_group_name}' already exists. Proceeding to the next step.")

# Create Storage Management Client
storage_client = StorageManagementClient(credential, subscription_id)

# Check if the storage account name is available
name_availability_params = StorageAccountCheckNameAvailabilityParameters(name=storage_account_name)
name_availability_result = storage_client.storage_accounts.check_name_availability(name_availability_params)
if name_availability_result.name_available:
    storage_account_params = StorageAccountCreateParameters(
        sku=Sku(name='Standard_LRS'),
        kind=Kind.STORAGE_V2,
        location=location
    )
    storage_client.storage_accounts.begin_create(resource_group_name, storage_account_name, storage_account_params).result()
    print(f"Storage account '{storage_account_name}' created.")
else:
    print(f"Storage account '{storage_account_name}' already exists. Proceeding to the next step.")

# Enable public access on the storage account
storage_account_update_params = StorageAccountUpdateParameters(
    allow_blob_public_access=True
)
storage_client.storage_accounts.update(resource_group_name, storage_account_name, storage_account_update_params)
print(f"Public access enabled for storage account '{storage_account_name}'.")

# Create Blob Service Client
blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net", credential=credential)

# Wait for 15 seconds
time.sleep(15)

# Check if the container exists
container_client = blob_service_client.get_container_client(container_name)
if not container_client.exists():
    # Create Container with anonymous access
    container_client = blob_service_client.create_container(container_name, public_access=PublicAccess.Container)
    print(f"Container '{container_name}' created with anonymous access.")
else:
    print(f"Container '{container_name}' already exists. Proceeding to the next step.")

# Run the datafactory.py script
subprocess.run(['python', 'adf.py'])