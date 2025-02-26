"""
Utils dealing with Azure Blob Storage and identity.
"""

from dataclasses import dataclass

import polars as pl
from azure.storage.blob import ContainerClient


@dataclass
class AzureStorage:
    """
    For some useful constants
    """

    AZURE_STORAGE_ACCOUNT_URL: str = "https://cfaazurebatchprd.blob.core.windows.net/"
    NSSP_CONTAINER_NAME: str = "nssp-etl"
    RT_CONTAINER_NAME: str = "zs-test-pipeline-update"
    SCOPE_URL: str = "https://cfaazurebatchprd.blob.core.windows.net/.default"


def scan_blob_parquet(
    container_client: ContainerClient, blob_name: str
) -> pl.LazyFrame:
    """
    Returns a LazyFrame reference to a parquet file in ABS.
    """
    return pl.scan_parquet(
        container_client.get_blob_client(blob_name).download_blob().readall()
    )
