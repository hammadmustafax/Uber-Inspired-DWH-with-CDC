import os
import sys
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from delta import configure_spark_with_delta_pip

sys.path.insert(0, os.path.abspath("."))
from src.databricks.silver.merge_bronze_to_silver import process_silver_scd1, process_silver_scd2

def main():
    print("=== Testing Silver Layer State Replay Logic ===")
    
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    dummy_hadoop_dir = os.path.abspath("data/hadoop").replace("\\", "/")
    os.makedirs(os.path.join(dummy_hadoop_dir, "bin"), exist_ok=True)
    os.environ["HADOOP_HOME"] = dummy_hadoop_dir
    os.environ["PATH"] = os.path.join(dummy_hadoop_dir, "bin") + ";" + os.environ.get("PATH", "")

    builder = SparkSession.builder \
        .appName("TestSilverMergeStandalone") \
        .master("local[2]") \
        .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={dummy_hadoop_dir}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.driver.host", "localhost")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    target_path = os.path.abspath("data/delta/test_silver_cities_standalone").replace("\\", "/")
    if os.path.exists(target_path):
        try:
            shutil.rmtree(target_path)
        except Exception:
            pass

    schema = StructType([
        StructField("op", StringType(), True),
        StructField("lsn", LongType(), True),
        StructField("source_ts_ms", LongType(), True),
        StructField("after", StringType(), True),
        StructField("before", StringType(), True)
    ])

    data = [
        ("c", 100, 1000, '{"city_id": 1, "city_name": "San Francisco", "country": "USA", "lat": 37.77, "lng": -122.41, "surge_enabled": true, "timezone": "PST"}', None),
        ("u", 105, 2000, '{"city_id": 1, "city_name": "San Francisco Bay", "country": "USA", "lat": 37.77, "lng": -122.41, "surge_enabled": true, "timezone": "PST"}', None)
    ]

    batch_df = spark.createDataFrame(data, schema)

    print(f"Executing process_silver_scd1 against {target_path}...")
    process_silver_scd1(spark, "cities", batch_df, target_path, "city_id")

    result_df = spark.read.format("delta").load(target_path)
    count = result_df.count()
    row = result_df.first()

    print("\n--- TEST RESULTS ---")
    print(f"Total Rows in Silver Cities Table: {count}")
    print(f"City Name: {row['city_name']}")
    print(f"Last CDC LSN: {row['last_cdc_lsn']}")
    print(f"Is Deleted: {row['is_deleted']}")

    assert count == 1, f"Expected 1 row, got {count}"
    assert row["city_name"] == "San Francisco Bay", f"Expected 'San Francisco Bay', got {row['city_name']}"
    assert row["last_cdc_lsn"] == 105, f"Expected LSN 105, got {row['last_cdc_lsn']}"

    print("\n[SUCCESS] ALL SILVER MERGE VERIFICATION TESTS PASSED SUCCESSFULLY!")

if __name__ == "__main__":
    main()
