import os
import sys
import unittest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType
from delta import configure_spark_with_delta_pip

# Import functions from Silver layer module
sys.path.insert(0, os.path.abspath("."))
from src.databricks.silver.merge_bronze_to_silver import process_silver_scd1, process_silver_scd2

class TestSilverMerge(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        
        dummy_hadoop_dir = os.path.abspath("data/hadoop").replace("\\", "/")
        os.makedirs(os.path.join(dummy_hadoop_dir, "bin"), exist_ok=True)
        os.environ["HADOOP_HOME"] = dummy_hadoop_dir
        os.environ["PATH"] = os.path.join(dummy_hadoop_dir, "bin") + ";" + os.environ.get("PATH", "")

        builder = SparkSession.builder \
            .appName("TestSilverMerge") \
            .master("local[2]") \
            .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={dummy_hadoop_dir}") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.sql.shuffle.partitions", "1") \
            .config("spark.driver.host", "localhost")
        
        cls.spark = configure_spark_with_delta_pip(builder).getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_scd1_merge(self):
        # Create mock Bronze micro-batch dataframe for 'cities'
        schema = StructType([
            StructField("op", StringType(), True),
            StructField("lsn", LongType(), True),
            StructField("source_ts_ms", LongType(), True),
            StructField("after", StringType(), True),
            StructField("before", StringType(), True)
        ])
        
        data = [
            ("c", 100, 1000, '{"city_id": 1, "city_name": "San Francisco", "country": "USA", "lat": 37.77, "lng": -122.41, "surge_enabled": true, "timezone": "PST"}', None)
        ]
        
        batch_df = self.spark.createDataFrame(data, schema)
        target_path = os.path.abspath("data/delta/test_silver_cities").replace("\\", "/")
        
        # Execute SCD1 merge
        process_silver_scd1(self.spark, "cities", batch_df, target_path, "city_id")
        
        # Verify Delta table creation and content
        result_df = self.spark.read.format("delta").load(target_path)
        self.assertEqual(result_df.count(), 1)
        self.assertEqual(result_df.first()["city_name"], "San Francisco")

if __name__ == "__main__":
    unittest.main()
