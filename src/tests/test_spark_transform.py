
# src/tests/test_spark_transform.py
from pyspark.sql import SparkSession
from spark_transform import transform_to_silver

def create_local_spark():
    return SparkSession.builder.master("local[2]").appName("test").getOrCreate()

def test_transform_to_silver_basic():
    spark = create_local_spark()
    data = [
        {"id":"1","name":" A ","brewery_type":"Micro","street":None,"city":"Testville","state":"CA","postal_code":"12345","country":"US","longitude":None,"latitude":None,"phone":None,"website_url":None,"updated_at":None},
    ]
    df = spark.createDataFrame(data)
    df2 = transform_to_silver(df)
    rows = df2.collect()
    assert rows[0]['name'] == "A"
    assert rows[0]['brewery_type'] == "micro"
    spark.stop()