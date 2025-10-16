import os
import json
import pandas as pd

# -----------------------------------------------------------
# Define expected schema (for schema enforcement)
# -----------------------------------------------------------
EXPECTED_COLUMNS = {
    "id": str,
    "name": str,
    "brewery_type": str,
    "street": str,
    "city": str,
    "state": str,
    "country": str,
    "longitude": str,
    "latitude": str,
}

def enforce_schema(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensures DataFrame follows the expected schema.
    Missing columns are added with empty values; unexpected columns are dropped.
    """
    for col, dtype in EXPECTED_COLUMNS.items():
        if col not in df.columns:
            df[col] = ""
        else:
            df[col] = df[col].astype(dtype)
    # Keep only expected columns
    return df[list(EXPECTED_COLUMNS.keys())]


# -----------------------------------------------------------
# Silver Layer Transformation
# -----------------------------------------------------------
def transform_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans, normalizes, and enforces schema for brewery data.
    """
    # Convert Spark DataFrame if necessary
    if hasattr(df, "toPandas"):
        df = df.toPandas()

    # Schema enforcement
    df = enforce_schema(df)

    # Clean text columns
    text_cols = ["name", "brewery_type", "city", "state", "country"]
    for col in text_cols:
        df[col] = df[col].astype(str).str.strip().str.title()

    # Normalize brewery_type to lowercase
    df["brewery_type"] = df["brewery_type"].str.lower()

    # Standardize missing values
    df = df.fillna("")

    # Drop duplicates based on brewery name and location
    df = df.drop_duplicates(subset=["name", "city", "state", "country"])

    return df


# -----------------------------------------------------------
# Gold Layer Aggregation
# -----------------------------------------------------------
def create_gold_aggregations(df_silver: pd.DataFrame) -> pd.DataFrame:
    """
    Creates an aggregated view: count of breweries per type and location.
    """
    gold_df = (
        df_silver.groupby(["country", "state", "brewery_type"])
        .size()
        .reset_index(name="brewery_count")
    )
    return gold_df


# -----------------------------------------------------------
# Pipeline Runner
# -----------------------------------------------------------
def run_transform(bronze_dir: str, silver_dir: str, gold_dir: str) -> str:
    """
    Reads bronze JSON, applies transformations, and writes Silver (Parquet) and Gold (aggregated) data.
    """
    os.makedirs(silver_dir, exist_ok=True)
    os.makedirs(gold_dir, exist_ok=True)

    # Find latest Bronze file
    bronze_files = sorted(
        [f for f in os.listdir(bronze_dir) if f.endswith(".json")],
        reverse=True
    )
    if not bronze_files:
        raise FileNotFoundError("No bronze JSON files found in directory.")

    #bronze_path = os.path.join(bronze_dir, bronze_files[0])
    bronze_path = os.path.join("data","lake","bronze", os.listdir("data/lake/bronze")[0])
    # Read Bronze
    with open(bronze_path, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)
    print(df[["country", "state"]].drop_duplicates())

    # Silver transformation
    df_silver = transform_to_silver(df)

    # If missing partition data, skip partitioning and just save one file
    if df_silver["country"].eq("").all() or df_silver["state"].eq("").all():
        silver_path = os.path.join(silver_dir, "silver_breweries.parquet")
        df_silver.to_parquet(silver_path, index=False, engine="pyarrow")
    else:
        silver_path = os.path.join(silver_dir)
        df_silver.to_parquet(
            silver_path,
            index=False,
            engine="pyarrow",
            partition_cols=["country", "state"],
        )

    print(f"Silver saved to: {silver_path}")

    # Gold transformation (aggregation)
    df_gold = create_gold_aggregations(df_silver)
    gold_path = os.path.join(gold_dir, "gold_breweries.parquet")
    df_gold.to_parquet(gold_path, index=False, engine="pyarrow")

    print(f"Silver saved to: {silver_path}")
    print(f"Gold saved to: {gold_path}")

    return silver_path