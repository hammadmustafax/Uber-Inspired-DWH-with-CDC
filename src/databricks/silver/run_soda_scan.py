import os
import sys
from pyspark.sql import SparkSession
# pyrefly: ignore [missing-import]
from delta.tables import DeltaTable
# pyrefly: ignore [missing-import]
from delta import configure_spark_with_delta_pip

def create_spark_session():
    """Initializes Spark session for Soda Core scan."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    dummy_hadoop_dir = os.path.abspath("data/hadoop").replace("\\", "/")
    os.makedirs(os.path.join(dummy_hadoop_dir, "bin"), exist_ok=True)
    os.environ["HADOOP_HOME"] = dummy_hadoop_dir
    os.environ["PATH"] = os.path.join(dummy_hadoop_dir, "bin") + ";" + os.environ.get("PATH", "")

    spark_temp_dir = os.path.abspath("data/spark_temp").replace("\\", "/")
    os.makedirs(spark_temp_dir, exist_ok=True)

    builder = SparkSession.builder \
        .appName("UberSilverSodaDataQualityScan") \
        .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={dummy_hadoop_dir}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.driver.host", "localhost") \
        .config("spark.local.dir", spark_temp_dir) \
        .config("spark.sql.shuffle.partitions", "1")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

def run_soda_scan():
    default_base_dir = "/app/data/delta" if os.path.exists("/app/data/delta") else os.path.abspath("data/delta")
    silver_target_dir = os.getenv("SILVER_TARGET_DIR", os.path.join(default_base_dir, "silver"))
    checks_file_path = os.getenv("SODA_CHECKS_FILE", "src/databricks/silver/soda_checks.yml")
    dry_run = "--dry-run" in sys.argv

    print("Starting Soda Core Data Quality Scan...")
    print(f"Silver Path: {silver_target_dir}")
    print(f"Soda Checks File: {checks_file_path}")

    spark = create_spark_session()

    if dry_run:
        print("Dry run requested. Spark session initialized successfully. Exiting.")
        spark.stop()
        sys.exit(0)

    # Register all Silver Delta tables as Spark temporary views for Soda scan
    silver_tables = ["users", "drivers", "trips", "driver_locations", "cities", "vehicles", "payments", "ratings", "promotions"]
    tables_registered = 0

    for table in silver_tables:
        table_path = os.path.join(silver_target_dir, f"silver_{table}").replace("\\", "/")
        if DeltaTable.isDeltaTable(spark, table_path):
            print(f"Registering temp view 'silver_{table}' from {table_path}")
            df = spark.read.format("delta").load(table_path)
            df.createOrReplaceTempView(f"silver_{table}")
            tables_registered += 1
        else:
            print(f"Warning: Silver table 'silver_{table}' not found at {table_path}. Skipping.")

    if tables_registered == 0:
        print("No Silver Delta tables found to scan. Exiting Soda scan.")
        spark.stop()
        return

    try:
        # pyrefly: ignore [missing-import]
        from soda.scan import Scan
    except ImportError:
        print("Note: 'soda-core' or 'soda-core-spark' is not installed in the current environment.")
        print("Tables successfully registered in Spark SQL. Skipping Soda scan execution.")
        spark.stop()
        return

    # Configure Soda Scan using Spark Session connection
    scan = Scan()
    scan.set_data_source_name("spark_df")
    scan.add_spark_session(spark, data_source_name="spark_df")

    if os.path.exists(checks_file_path):
        scan.add_sodacl_yaml_file(checks_file_path)
    else:
        print(f"Error: Soda checks file not found at {checks_file_path}")
        sys.exit(1)

    print("Executing Soda Scan...")
    exit_code = scan.execute()
    
    print("\n--- Soda Scan Results Summary ---")
    print(scan.get_logs_text())
    
    if exit_code != 0:
        print("Data Quality checks failed or raised warnings!")
        sys.exit(exit_code)
    else:
        print("All Data Quality checks passed successfully!")

if __name__ == "__main__":
    run_soda_scan()
