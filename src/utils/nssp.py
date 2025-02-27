"""
Utils for pulling data from NSSP ETL.
"""

from dataclasses import dataclass
from datetime import date

import polars as pl
from azure.identity import DefaultAzureCredential
from azure.storage.blob import ContainerClient

from src.utils.az import AzureStorage, scan_blob_parquet


@dataclass
class NSSP:
    GEO_COL: str = "geo_value"
    DISEASE_COL: str = "disease"
    FLU_KEY: str = "Influenza"
    COVID_KEY: str = "COVID-19"


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
    except Exception:
        raise ValueError(f"Failed to pull gold data for {full_path}.")


def filter_geography(
    data: pl.LazyFrame, geo_values: list | None = None
) -> pl.LazyFrame:
    """
    Filters a LazyFrame to a given list of geographies.
    Args:
        data (pl.LazyFrame): NSSP gold data to filter
        geo_values (list): list of geographies to filter to
    Returns:
        pl.LazyFrame: parquet reference to filtered dataset
    """
    if not geo_values:
        return data

    return data.filter(pl.col(NSSP.GEO_COL).is_in(geo_values))


def parse_disease(s: str) -> str:
    """
    Converts a string into a valid disease string

    Raises:
        ValueError: If the input string is not a valid disease
    """
    if s.lower() in ["covid", "covid19", "covid-19", "covid_19", "covid 19"]:
        return NSSP.COVID_KEY
    elif s.lower() in ["flu", "influenza"]:
        return NSSP.FLU_KEY
    else:
        raise ValueError(
            f"Could not parse disease name: {s}. Expecting one of {NSSP.COVID_KEY}, {NSSP.FLU_KEY}"
        )


def filter_disease(data: pl.LazyFrame, disease: str | None = None) -> pl.LazyFrame:
    """
    Filters a LazyFrame to a given disease.
    Args:
        data (pl.LazyFrame): NSSP gold data to filter
        disease (str): list of disease to filter to
    Returns:
        pl.LazyFrame: parquet reference to filtered dataset
    """
    if not disease:
        return data

    return data.filter(pl.col(NSSP.DISEASE_COL) == parse_disease(disease))
