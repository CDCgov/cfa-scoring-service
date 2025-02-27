"""
Utils for pulling data from NSSP ETL.
"""

import polars as pl
from src.utils.az import AzureStorage, scan_blob_parquet
from azure.storage.blob import ContainerClient
from azure.identity import DefaultAzureCredential

def get_nssp_gold(path: str) -> pl.LazyFrame:
    container_client = ContainerClient(account_url=AzureStorage.AZURE_STORAGE_ACCOUNT_URL, container_name=AzureStorage.NSSP_CONTAINER_NAME, credential=DefaultAzureCredential())
