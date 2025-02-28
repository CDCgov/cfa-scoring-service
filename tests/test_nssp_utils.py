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


@pytest.mark.parametrize(
    "disease, expected, should_fail",
    [
        ("covid19", NSSP.COVID_KEY, False),
        ("covid", NSSP.COVID_KEY, False),
        ("covid-19", NSSP.COVID_KEY, False),
        ("COVID-19", NSSP.COVID_KEY, False),
        ("COVID", NSSP.COVID_KEY, False),
        ("flu", NSSP.FLU_KEY, False),
        ("Flu", NSSP.FLU_KEY, False),
        ("influenza", NSSP.FLU_KEY, False),
        ("Influenza", NSSP.FLU_KEY, False),
        ("corvid19", None, True),
        ("", None, True),
        ("influ", None, True),
        ("flu-19", None, True),
        ("Flu-19", None, True),
    ],
)
def test_parse_disease(disease, expected, should_fail):
    if should_fail:
        with pytest.raises(ValueError):
            parse_disease(disease)
    else:
        parsed_disease = parse_disease(disease)
        assert parsed_disease == expected
        assert isinstance(parsed_disease, str)
