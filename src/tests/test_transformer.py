# src/tests/test_transform_no_spark.py
import pandas as pd

import sys
sys.path.append("/opt/airflow/dags")

from transformer import transform_to_silver

def test_transform_to_silver_basic():
    df = pd.DataFrame([
        {"id": "1", "name": " A ", "brewery_type": "Micro"}
    ])

    df2 = transform_to_silver(df)

    assert df2.loc[0, "name"] == "A"
    assert df2.loc[0, "brewery_type"] == "micro"