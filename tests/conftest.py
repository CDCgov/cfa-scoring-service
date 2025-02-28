import polars as pl
import pytest


@pytest.fixture
def nssp_gold_df() -> pl.LazyFrame:
    return pl.scan_parquet("tests/data/nssp_gold_test_data.parquet")
