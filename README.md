# Day 3 DevOps challenge

This script automates the creation and configuration of an Azure Storage Account and Blob Container, including enabling public access and running an additional script (`adf.py`).
The goal is to create a data factory that invokes request to sportsdata.io players info

## Prerequisites

- Python 3.x
- Azure SDK for Python
- `dotenv` package for loading environment variables
- Azure subscription with appropriate permissions

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/annoyedalien/Day3.git
    cd repository
    ```
2. Create a virtual environment
   ```sh
   source -m venv (your venv)
   python (your venv)\bin\activate
   
2. Install the required Python packages:
    ```sh
    pip install -r requirements.txt
    ```

3. Create a `.env` file in the root directory of the project and add the following environment variables:
    ```env
    AZURE_SUBSCRIPTION_ID=your_subscription_id
    RESOURCE_GROUP_NAME=your_resource_group_name
    STORAGE_ACCOUNT_NAME=your_storage_account_name
    LOCATION=your_location
    CONTAINER_NAME=your_container_name
    DATA_FACTORY_NAME=your_datafactory_name
    REST_API_URL=https://api.sportsdata.io/v3/nba/scores/json/Players
    SUBSCRIPTION_KEY=your_api_key
    LS_REST_NAME=linked_service_rest_name
    LS_BLOB_NAME=linked_service_blob_name
    ```

## Usage

1. Run the script:
    ```sh
    main.py
    ```

2. The script will:
    - Check if the specified resource group exists and create it if it doesn't.
    - Check if the specified storage account name is available and create the storage account if it doesn't exist.
    - Enable public access on the storage account.
    - Create a blob container with anonymous access if it doesn't exist.
    - Run the `adf.py` script.
    - adf.py will create data factory,linked services,data sets and pipelines
    - config.py contains the properties of the resources to be created by adf.py

3. Run the pipeline in datafactory studio
    - check the contents sent on the blob storage.

## Script Details

- **Resource Management**: Uses `ResourceManagementClient` to manage Azure resource groups.
- **Storage Management**: Uses `StorageManagementClient` to manage Azure storage accounts.
- **Blob Service**: Uses `BlobServiceClient` to manage blob containers and enable public access.
- **Environment Variables**: Loads configuration from a `.env` file using the `dotenv` package.
- **Subprocess**: Runs an additional script (`adf.py`) after setting up the storage account and container.

##adf.py and config.py
- **adf.py** contains the creation of azure data factory together with its linked services,datasets and pipelines
- **config.py** contains the properties of the linked services,datasets and pipelines.

## Contributing

Contributions are welcome! Please open an issue or submit a pull request with your changes.

