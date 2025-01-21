
from azure.identity import DefaultAzureCredential
from azure.mgmt.datafactory import DataFactoryManagementClient
from azure.mgmt.datafactory.models import *
from azure.mgmt.datafactory.models import Factory
import config


# Authenticate and create a Data Factory management client
credential = DefaultAzureCredential()
adf_client = DataFactoryManagementClient(credential, config.subscription_id)

# Create or update the Data Factory
df_resource = Factory(location='southeast asia')
adf_client.factories.create_or_update(
config.resource_group_name,
config.data_factory_name, 
df_resource)

print(f"Data Factory '{config.data_factory_name}' created or updated.")



# Create or update the linked service REST
adf_client.linked_services.create_or_update(
    config.resource_group_name,
    config.data_factory_name,
    config.ls_rest_name,
    {
        'properties': config.ls_properties_rest
    }
)

print(f"Linked service '{config.ls_rest_name}' created/updated successfully.")


# Create or update the linked service BLOB
adf_client.linked_services.create_or_update(
    config.resource_group_name,
    config.data_factory_name,
    config.ls_blob_name,
    {
        'properties': config.ls_properties_blob
    }
)

print(f"Linked service '{config.ls_blob_name}'  created/updated successfully.")

# Create or update the REST dataset
rest_dataset_name = "RestDataset"
adf_client.datasets.create_or_update(
    config.resource_group_name,
    config.data_factory_name,
    rest_dataset_name,
    {
        'properties': config.rest_dataset_properties
    }
)

print(f"Dataset '{rest_dataset_name}' created/updated successfully.")


# Create or update the Blob dataset
blob_dataset_name = "BlobDataset"
adf_client.datasets.create_or_update(
    config.resource_group_name,
    config.data_factory_name,
    blob_dataset_name,
    {
        'properties': config.blob_dataset_properties
    }
)

print(f"Dataset '{blob_dataset_name}' created/updated successfully.")

# Create or update the pipeline
pipeline_name = "MyPipeline"
adf_client.pipelines.create_or_update(
    config.resource_group_name,
    config.data_factory_name,
    pipeline_name,
    {
        'properties': config.pipeline_properties
    }
)

print(f"Pipeline '{pipeline_name}' created/updated successfully.")