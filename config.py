from azure.identity import DefaultAzureCredential
from azure.mgmt.storage import StorageManagementClient
from dotenv import load_dotenv
import os

load_dotenv()
# Retrieve environment variables

subscription_id = os.getenv('AZURE_SUBSCRIPTION_ID')
resource_group_name = os.getenv('RESOURCE_GROUP_NAME')
data_factory_name = os.getenv('DATA_FACTORY_NAME')
rest_api_url = os.getenv('REST_API_URL')
container_name = os.getenv('CONTAINER_NAME')
ls_rest_name = os.getenv('LS_REST_NAME')
ls_blob_name = os.getenv('LS_BLOB_NAME')
subscription_key = os.getenv('SUBSCRIPTION_KEY')
storage_account_name =  os.getenv('STORAGE_ACCOUNT_NAME')
bcs = os.getenv('BCS')
blob_folder_path = os.getenv('BLOB_FOLDER_PATH')
blob_file_name = os.getenv('BLOB_FILE_NAME')



# Authenticate using DefaultAzureCredential
credential = DefaultAzureCredential()

# Create a StorageManagementClient
storage_client = StorageManagementClient(credential, subscription_id)

# Get the storage account keys
keys = storage_client.storage_accounts.list_keys(resource_group_name, storage_account_name)

# Retrieve the connection string
connection_string = f"DefaultEndpointsProtocol=https;AccountName={storage_account_name};AccountKey={keys.keys[0].value};EndpointSuffix=core.windows.net"

print(f"Connection String: {connection_string}")

# Linked service properties REST
ls_properties_rest = {
    "type": "RestService",
    "typeProperties": {
        "url": "https://api.sportsdata.io/v3/nba/scores/json/Players",
        
        "authenticationType": "Anonymous",
        "authHeaders": {
            "Ocp-Apim-Subscription-Key": subscription_key,
            
            },
        "enableServerCertificateValidation": False,       
}
}

# Linked service properties for Azure Blob Storage
ls_properties_blob = {
    "type": "AzureBlobStorage",
    "typeProperties": {
        "connectionString": connection_string
    }
}

# REST dataset properties
rest_dataset_properties = {
    "type": "RestResource",
    "linkedServiceName": {
        "referenceName": ls_rest_name,
        "type": "LinkedServiceReference"
    },
    "typeProperties": {
        "requestMethod": "GET"
    }
}


# BLOB dataset properties
blob_dataset_properties = {
    
    "type": "Json",
        "linkedServiceName": {
            "referenceName": ls_blob_name,
             "type": "LinkedServiceReference",
            
        },
        "annotations": [],
        "type": "Json",
        "typeProperties": {
            "location": {
                "type": "AzureBlobStorageLocation",
                "fileName": "player.json",
                "container": container_name,
            }
        },
        "schema": {}
    
}


# Pipeline properties
pipeline_properties = {
    "activities": [
        {
            "name": "CopyFromRestToBlob",
            "type": "Copy",
            "inputs": [
                {
                    "referenceName": "RestDataset",
                    "type": "DatasetReference"
                }
            ],
            "outputs": [
                {
                    "referenceName": "BlobDataset",
                    "type": "DatasetReference"
                }
            ],
            "typeProperties": {
                "source": {
                    "type": "RestSource"
                },
                "sink": {
                    "type": "BlobSink"
                },
                "enableStaging": False,
                "translator": {
                    "type": "TabularTranslator",
                    "mappings": [
                        {
                            "source": {
                                "path": "$['Status']"
                            },
                            "sink": {
                                "path": "$['Status']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['TeamID']"
                            },
                            "sink": {
                                "path": "$['TeamID']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['Team']"
                            },
                            "sink": {
                                "path": "$['Team']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['Jersey']"
                            },
                            "sink": {
                                "path": "$['Jersey']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['PositionCategory']"
                            },
                            "sink": {
                                "path": "$['PositionCategory']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['Position']"
                            },
                            "sink": {
                                "path": "$['Position']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['FirstName']"
                            },
                            "sink": {
                                "path": "$['FirstName']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['LastName']"
                            },
                            "sink": {
                                "path": "$['LastName']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['Height']"
                            },
                            "sink": {
                                "path": "$['Height']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['Weight']"
                            },
                            "sink": {
                                "path": "$['Weight']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['BirthDate']"
                            },
                            "sink": {
                                "path": "$['BirthDate']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['BirthCity']"
                            },
                            "sink": {
                                "path": "$['BirthCity']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['BirthState']"
                            },
                            "sink": {
                                "path": "$['BirthState']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['BirthCountry']"
                            },
                            "sink": {
                                "path": "$['BirthCountry']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['HighSchool']"
                            },
                            "sink": {
                                "path": "$['HighSchool']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['College']"
                            },
                            "sink": {
                                "path": "$['College']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['Salary']"
                            },
                            "sink": {
                                "path": "$['Salary']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['PhotoUrl']"
                            },
                            "sink": {
                                "path": "$['PhotoUrl']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['Experience']"
                            },
                            "sink": {
                                "path": "$['Experience']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['InjuryStatus']"
                            },
                            "sink": {
                                "path": "$['InjuryStatus']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['InjuryBodyPart']"
                            },
                            "sink": {
                                "path": "$['InjuryBodyPart']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['InjuryStartDate']"
                            },
                            "sink": {
                                "path": "$['InjuryStartDate']"
                            }
                        },
                        {
                            "source": {
                                "path": "$['InjuryNotes']"
                            },
                            "sink": {
                                "path": "$['InjuryNotes']"
                            }
                        }
                    ]
                }
            }
        }
    ],
    "annotations": []
}


