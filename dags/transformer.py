# src/transformer.py
import os
import json
import pandas as pd

def transform_to_silver(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans and normalizes brewery data.
    Works on both Spark-converted (to pandas) or native pandas DataFrames.
    """

    # If Spark DataFrame, convert to pandas
    if hasattr(df, "toPandas"):
        df = df.toPandas()

    # Clean text columns
    df["name"] = df["name"].astype(str).str.strip()
    df["brewery_type"] = df["brewery_type"].astype(str).str.lower()

    # Standardize missing values
    df = df.fillna("")

    # Return the transformed DataFrame
    return df


def run_transform(bronze_dir: str, silver_dir: str, gold_dir: str) -> str:
    """
    Reads the bronze file, transforms data, and writes silver and gold outputs.
    """
    os.makedirs(silver_dir, exist_ok=True)
    os.makedirs(gold_dir, exist_ok=True)

    # Find the latest bronze file
    bronze_files = sorted(
        [f for f in os.listdir(bronze_dir) if f.endswith(".json")],
        reverse=True
    )
    if not bronze_files:
        raise FileNotFoundError("No bronze JSON files found in directory.")

    bronze_path = os.path.join(bronze_dir, bronze_files[0])

    # Read bronze data
    with open(bronze_path, "r") as f:
        data = json.load(f)

    df = pd.DataFrame(data)

    # Apply transformations
    df_silver = transform_to_silver(df)

    # Save silver output
    silver_path = os.path.join(silver_dir, "silver_breweries.csv")
    df_silver.to_csv(silver_path, index=False)
    print(silver_path)
    # Example gold layer: just reuse silver for now (placeholder)
    gold_path = os.path.join(gold_dir, "gold_breweries.csv")
    df_silver.to_csv(gold_path, index=False)
    print(gold_path)
    return silver_path
