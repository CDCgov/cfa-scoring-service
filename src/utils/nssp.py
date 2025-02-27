"""
Utils for pulling data from NSSP ETL.
"""

from datetime import date

import polars as pl
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient

from src.utils.az import AzureStorage, scan_blob_parquet


def get_nssp_gold(report_date: date) -> pl.LazyFrame:
    """
    Pulls NSSP gold data for a given report date. Errors if the date is not found.
    Args:
        report_date (date): report date for which to pull data
    Returns:
        pl.LazyFrame: parquet reference to NSSP gold data
    """
    container_client = ContainerClient(
        account_url=AzureStorage.AZURE_STORAGE_ACCOUNT_URL,
        container_name=AzureStorage.NSSP_CONTAINER_NAME,
        credential=DefaultAzureCredential(),
    )
    full_path = f"gold/{report_date.isoformat()}.parquet"
    try:
        gold_df = scan_blob_parquet(
            container_client=container_client, blob_name=full_path
        )
        return gold_df
    except Exception as e:
        raise ValueError(f"Failed to pull gold data for {full_path}.")
