import pandas as pd
import pytest
from transformer import (
    transform_to_silver,
    create_gold_aggregations,
    enforce_schema,
)

# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------
@pytest.fixture
def sample_data():
    """Provide a sample raw dataset similar to bronze input."""
    return pd.DataFrame([
        {
            "id": "1",
            "name": "brew co",
            "brewery_type": "Micro",
            "street": "123 Beer St",
            "city": "Portland",
            "state": "Oregon",
            "country": "United States",
            "longitude": "-122.6",
            "latitude": "45.5",
        },
        {
            "id": "2",
            "name": "brew co",  # duplicate name
            "brewery_type": "MICRO",
            "street": "123 Beer St",
            "city": "Portland",
            "state": "Oregon",
            "country": "United States",
            "longitude": "-122.6",
            "latitude": "45.5",
        },
        {
            "id": "3",
            "name": "Hopsters",
            "brewery_type": "BrewPub",
            "street": "456 Hop Ln",
            "city": "Austin",
            "state": "Texas",
            "country": "United States",
            "longitude": "-97.7",
            "latitude": "30.2",
        },
    ])


# -----------------------------------------------------------------------------
# Tests for Schema Enforcement
# -----------------------------------------------------------------------------
def test_enforce_schema_adds_missing_columns():
    """Ensure missing columns are added with correct dtypes."""
    df = pd.DataFrame([{"name": "Sample Brewery"}])
    enforced = enforce_schema(df)
    for col in ["id", "country", "state", "city", "brewery_type"]:
        assert col in enforced.columns, f"Column '{col}' should be added"
    assert all(col in enforced.columns for col in enforce_schema(df).columns)


# -----------------------------------------------------------------------------
# Tests for Silver Transformation
# -----------------------------------------------------------------------------
def test_transform_to_silver_cleaning(sample_data):
    """Ensure text fields are properly normalized and cleaned."""
    df_silver = transform_to_silver(sample_data)

    # Names should be title-cased
    assert all(df_silver["name"] == df_silver["name"].str.title()), "Names should be title-cased"

    # Brewery types should be lowercase
    assert all(df_silver["brewery_type"].str.islower()), "Brewery types should be lowercase"

    # There should be no duplicates by (name, city, state, country)
    duplicates = df_silver.duplicated(subset=["name", "city", "state", "country"]).sum()
    assert duplicates == 0, f"Found {duplicates} duplicates in cleaned data"

    # No nulls in critical fields
    for col in ["name", "brewery_type", "city", "state", "country"]:
        assert not df_silver[col].isnull().any(), f"Null values found in {col}"


# -----------------------------------------------------------------------------
# Tests for Gold Aggregations
# -----------------------------------------------------------------------------
def test_create_gold_aggregations_counts(sample_data):
    """Verify aggregation of brewery count per type and location."""
    df_silver = transform_to_silver(sample_data)
    df_gold = create_gold_aggregations(df_silver)

    # Columns should be correct
    expected_cols = {"country", "state", "brewery_type", "brewery_count"}
    assert set(df_gold.columns) == expected_cols, "Gold layer columns mismatch"

    # Verify that brewery_count is aggregated correctly
    total_count = df_gold["brewery_count"].sum()
    assert total_count == len(df_silver), "Total aggregated count mismatch"

    # Ensure there are no nulls in key columns
    assert not df_gold[["country", "state", "brewery_type"]].isnull().any().any(), "Nulls in gold keys"

    # Ensure counts are positive integers
    assert (df_gold["brewery_count"] > 0).all(), "Invalid brewery counts in gold layer"