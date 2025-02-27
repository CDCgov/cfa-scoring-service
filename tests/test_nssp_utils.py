import polars as pl
import pytest

from src.utils.nssp import NSSP, filter_disease, filter_geography, parse_disease


def test_filter_geography(nssp_gold_df: pl.LazyFrame):
    geo_values = ["FL", "WA"]
    df_filtered = filter_geography(nssp_gold_df, geo_values)
    filtered_geo = pl.Series(
        df_filtered.collect().select("geo_value").unique()
    ).to_list()

    # Convert to set for stable comparison
    assert set(geo_values) == set(filtered_geo)


def test_filter_geography_empty(nssp_gold_df: pl.LazyFrame):
    geo_values = []
    df_filtered = filter_geography(nssp_gold_df, geo_values)
    assert nssp_gold_df.collect().equals(df_filtered.collect())


def test_filter_geography_none(nssp_gold_df: pl.LazyFrame):
    geo_values = None
    df_filtered = filter_geography(nssp_gold_df, geo_values)
    assert nssp_gold_df.collect().equals(df_filtered.collect())


def test_filter_disease(nssp_gold_df: pl.LazyFrame):
    disease = "Influenza"
    df_filtered = filter_disease(nssp_gold_df, disease)
    filtered_disease = pl.Series(
        df_filtered.collect().select("disease").unique()
    ).to_list()
    assert set(filtered_disease) == set([disease])


def test_filter_disease_none(nssp_gold_df: pl.LazyFrame):
    disease = None
    df_filtered = filter_disease(nssp_gold_df, disease)
    assert nssp_gold_df.collect().equals(df_filtered.collect())


def test_parse_disease_covid():
    disease = "covid19"
    parsed_disease = parse_disease(disease)
    assert parsed_disease == NSSP.COVID_KEY


def test_parse_disease_flu():
    disease = "flu"
    parsed_disease = parse_disease(disease)
    assert parsed_disease == NSSP.FLU_KEY


def test_parse_disease_invalid():
    disease = "corvid19"
    with pytest.raises(ValueError):
        parse_disease(disease)
