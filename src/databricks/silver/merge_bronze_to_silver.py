import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, current_timestamp, row_number, when, lit, coalesce
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, DoubleType, BooleanType, IntegerType
)

# pyrefly: ignore [missing-import]
from delta.tables import DeltaTable
# pyrefly: ignore [missing-import]
from delta import configure_spark_with_delta_pip

def create_spark_session():
    """Initializes the Spark session with Delta Lake support."""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    dummy_hadoop_dir = os.path.abspath("data/hadoop").replace("\\", "/")
    os.makedirs(os.path.join(dummy_hadoop_dir, "bin"), exist_ok=True)
    os.environ["HADOOP_HOME"] = dummy_hadoop_dir
    os.environ["PATH"] = os.path.join(dummy_hadoop_dir, "bin") + ";" + os.environ.get("PATH", "")

    builder = SparkSession.builder \
        .appName("UberSilverStateReplay") \
        .config("spark.driver.extraJavaOptions", f"-Dhadoop.home.dir={dummy_hadoop_dir}") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.driver.host", "localhost")
    
    return configure_spark_with_delta_pip(builder).getOrCreate()

# Entity schemas matching Debezium JSON 'after' / 'before' payloads
ENTITY_SCHEMAS = {
    "cities": StructType([
        StructField("city_id", IntegerType(), True),
        StructField("city_name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("surge_enabled", BooleanType(), True),
        StructField("timezone", StringType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True)
    ]),
    "users": StructType([
        StructField("user_id", IntegerType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("signup_at", StringType(), True),
        StructField("home_city_id", IntegerType(), True),
        StructField("updated_at", StringType(), True)
    ]),
    "vehicles": StructType([
        StructField("vehicle_id", IntegerType(), True),
        StructField("driver_id", IntegerType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("plate_number", StringType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("updated_at", StringType(), True)
    ]),
    "drivers": StructType([
        StructField("driver_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("license_number", StringType(), True),
        StructField("vehicle_id", IntegerType(), True),
        StructField("status", StringType(), True),
        StructField("rating_avg", DoubleType(), True),
        StructField("onboarded_at", StringType(), True),
        StructField("updated_at", StringType(), True)
    ]),
    "trip_requests": StructType([
        StructField("request_id", IntegerType(), True),
        StructField("rider_id", IntegerType(), True),
        StructField("city_id", IntegerType(), True),
        StructField("pickup_lat", DoubleType(), True),
        StructField("pickup_lng", DoubleType(), True),
        StructField("dropoff_lat", DoubleType(), True),
        StructField("dropoff_lng", DoubleType(), True),
        StructField("requested_vehicle_type", StringType(), True),
        StructField("requested_at", StringType(), True),
        StructField("estimated_fare", DoubleType(), True),
        StructField("surge_multiplier", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("updated_at", StringType(), True)
    ]),
    "trips": StructType([
        StructField("trip_id", IntegerType(), True),
        StructField("request_id", IntegerType(), True),
        StructField("rider_id", IntegerType(), True),
        StructField("driver_id", IntegerType(), True),
        StructField("vehicle_id", IntegerType(), True),
        StructField("pickup_lat", DoubleType(), True),
        StructField("pickup_lng", DoubleType(), True),
        StructField("dropoff_lat", DoubleType(), True),
        StructField("dropoff_lng", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("requested_at", StringType(), True),
        StructField("matched_at", StringType(), True),
        StructField("driver_arrived_at", StringType(), True),
        StructField("started_at", StringType(), True),
        StructField("completed_at", StringType(), True),
        StructField("cancelled_at", StringType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("base_fare", DoubleType(), True),
        StructField("surge_multiplier", DoubleType(), True),
        StructField("final_fare", DoubleType(), True),
        StructField("cancellation_reason", StringType(), True),
        StructField("cancelled_by", StringType(), True),
        StructField("updated_at", StringType(), True)
    ]),
    "driver_locations": StructType([
        StructField("location_id", LongType(), True),
        StructField("driver_id", IntegerType(), True),
        StructField("trip_id", IntegerType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lng", DoubleType(), True),
        StructField("heading", DoubleType(), True),
        StructField("speed_kmh", DoubleType(), True),
        StructField("recorded_at", StringType(), True)
    ]),
    "payments": StructType([
        StructField("payment_id", IntegerType(), True),
        StructField("trip_id", IntegerType(), True),
        StructField("rider_id", IntegerType(), True),
        StructField("amount", DoubleType(), True),
        StructField("payment_method", StringType(), True),
        StructField("status", StringType(), True),
        StructField("processed_at", StringType(), True),
        StructField("updated_at", StringType(), True)
    ]),
    "ratings": StructType([
        StructField("rating_id", IntegerType(), True),
        StructField("trip_id", IntegerType(), True),
        StructField("rated_by", StringType(), True),
        StructField("rating_value", IntegerType(), True),
        StructField("comment", StringType(), True),
        StructField("created_at", StringType(), True)
    ]),
    "promotions": StructType([
        StructField("promo_id", IntegerType(), True),
        StructField("rider_id", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("discount_type", StringType(), True),
        StructField("discount_value", DoubleType(), True),
        StructField("applied_trip_id", IntegerType(), True),
        StructField("applied_at", StringType(), True)
    ])
}

# Primary key mapping per domain entity
PRIMARY_KEYS = {
    "cities": "city_id",
    "users": "user_id",
    "vehicles": "vehicle_id",
    "drivers": "driver_id",
    "trip_requests": "request_id",
    "trips": "trip_id",
    "driver_locations": "location_id",
    "payments": "payment_id",
    "ratings": "rating_id",
    "promotions": "promo_id"
}

# Entities tracked as SCD Type 2 history in Silver
SCD_TYPE_2_TABLES = {"users", "drivers"}

def process_silver_scd1(spark, entity_name, batch_df, target_path, pk_col):
    """Processes micro-batch merge for standard SCD Type 1 entities."""
    schema = ENTITY_SCHEMAS[entity_name]
    
    # 1. Parse JSON payload ('after' for insert/update, 'before' for delete)
    parsed_df = batch_df.withColumn(
        "payload_raw",
        when(col("op") == "d", col("before")).otherwise(col("after"))
    ).withColumn(
        "data", from_json(col("payload_raw"), schema)
    )
    
    # Filter out records where data payload couldn't be parsed
    valid_df = parsed_df.filter(col("data").isNotNull())
    
    if valid_df.count() == 0:
        return

    # Expand payload columns
    expanded_df = valid_df.select(
        col("data.*"),
        col("op").alias("last_cdc_op"),
        col("lsn").alias("last_cdc_lsn"),
        col("source_ts_ms").alias("source_ts_ms"),
        when(col("op") == "d", lit(True)).otherwise(lit(False)).alias("is_deleted"),
        current_timestamp().alias("_silver_processed_at")
    )
    
    # 2. Intra-Batch Deduplication: Keep the event with the highest LSN per Primary Key
    window_spec = Window.partitionBy(pk_col).orderBy(col("last_cdc_lsn").desc(), col("source_ts_ms").desc())
    dedup_df = expanded_df.withColumn("rn", row_number().over(window_spec)) \
                          .filter(col("rn") == 1) \
                          .drop("rn")

    # 3. Create or Merge into Target Delta Table
    if not DeltaTable.isDeltaTable(spark, target_path):
        print(f"Creating Silver Delta table: {target_path}")
        dedup_df.write.format("delta").mode("overwrite").save(target_path)
    else:
        target_table = DeltaTable.forPath(spark, target_path)
        
        # Build dynamic update/insert dictionaries
        field_names = [f.name for f in schema.fields] + ["last_cdc_op", "last_cdc_lsn", "source_ts_ms", "is_deleted", "_silver_processed_at"]
        update_set = {field: f"source.{field}" for field in field_names}
        insert_set = {field: f"source.{field}" for field in field_names}
        
        # Execute MERGE with LSN Sequence Guard to protect against out-of-order events
        target_table.alias("target") \
            .merge(
                source=dedup_df.alias("source"),
                condition=f"target.{pk_col} = source.{pk_col}"
            ) \
            .whenMatchedUpdate(
                condition="source.last_cdc_lsn > target.last_cdc_lsn",
                set=update_set
            ) \
            .whenNotMatchedInsert(
                values=insert_set
            ) \
            .execute()

def process_silver_scd2(spark, entity_name, batch_df, target_path, pk_col):
    """Processes micro-batch merge with SCD Type 2 history tracking for users & drivers."""
    schema = ENTITY_SCHEMAS[entity_name]
    
    parsed_df = batch_df.withColumn(
        "payload_raw",
        when(col("op") == "d", col("before")).otherwise(col("after"))
    ).withColumn(
        "data", from_json(col("payload_raw"), schema)
    )
    
    valid_df = parsed_df.filter(col("data").isNotNull())
    if valid_df.count() == 0:
        return

    expanded_df = valid_df.select(
        col("data.*"),
        col("op").alias("last_cdc_op"),
        col("lsn").alias("last_cdc_lsn"),
        col("source_ts_ms").alias("source_ts_ms"),
        when(col("op") == "d", lit(True)).otherwise(lit(False)).alias("is_deleted"),
        current_timestamp().alias("_silver_processed_at")
    )
    
    # Deduplicate within batch
    window_spec = Window.partitionBy(pk_col).orderBy(col("last_cdc_lsn").desc(), col("source_ts_ms").desc())
    dedup_df = expanded_df.withColumn("rn", row_number().over(window_spec)) \
                          .filter(col("rn") == 1) \
                          .drop("rn") \
                          .withColumn("is_current", lit(True)) \
                          .withColumn("effective_start_date", current_timestamp()) \
                          .withColumn("effective_end_date", lit(None).cast("timestamp"))

    if not DeltaTable.isDeltaTable(spark, target_path):
        print(f"Creating Silver SCD2 Delta table: {target_path}")
        dedup_df.write.format("delta").mode("overwrite").save(target_path)
    else:
        target_table = DeltaTable.forPath(spark, target_path)
        
        # Merge pattern: Close out active record if modified, then insert new version
        field_names = [f.name for f in schema.fields] + [
            "last_cdc_op", "last_cdc_lsn", "source_ts_ms", "is_deleted",
            "_silver_processed_at", "is_current", "effective_start_date", "effective_end_date"
        ]
        
        # 1. Close out existing current record when matched with newer LSN
        target_table.alias("target") \
            .merge(
                source=dedup_df.alias("source"),
                condition=f"target.{pk_col} = source.{pk_col} AND target.is_current = true"
            ) \
            .whenMatchedUpdate(
                condition="source.last_cdc_lsn > target.last_cdc_lsn",
                set={
                    "is_current": "false",
                    "effective_end_date": "source.effective_start_date",
                    "_silver_processed_at": "current_timestamp()"
                }
            ) \
            .execute()

        # 2. Insert new current record
        insert_set = {field: f"source.{field}" for field in field_names}
        target_table.alias("target") \
            .merge(
                source=dedup_df.alias("source"),
                condition=f"target.{pk_col} = source.{pk_col} AND target.last_cdc_lsn = source.last_cdc_lsn"
            ) \
            .whenNotMatchedInsert(
                values=insert_set
            ) \
            .execute()

def main():
    # Resolve default paths dynamically: use /app if inside Docker container, else fallback to workspace local directory 'data/delta'
    default_base_dir = "/app/data/delta" if os.path.exists("/app/data/delta") else os.path.abspath("data/delta")
    
    bronze_target_dir = os.getenv("BRONZE_TARGET_DIR", os.path.join(default_base_dir, "bronze"))
    silver_target_dir = os.getenv("SILVER_TARGET_DIR", os.path.join(default_base_dir, "silver"))
    checkpoint_dir = os.getenv("CHECKPOINT_DIR", os.path.join(default_base_dir, "checkpoints", "silver"))
    streaming_once = os.getenv("STREAMING_ONCE", "false").lower() == "true"
    dry_run = "--dry-run" in sys.argv

    print("Starting Silver State Replay Layer...")
    print(f"Bronze Path: {bronze_target_dir}")
    print(f"Silver Path: {silver_target_dir}")
    print(f"Checkpoint Path: {checkpoint_dir}")
    print(f"Mode: {'Trigger AvailableNow (Batch)' if streaming_once else 'Continuous'}")

    spark = create_spark_session()

    if dry_run:
        print("Dry run requested. Spark session initialized successfully. Exiting.")
        spark.stop()
        sys.exit(0)

    # Process all 10 domain entities from Bronze Delta tables to Silver
    for entity_name, pk_col in PRIMARY_KEYS.items():
        bronze_path = os.path.join(bronze_target_dir, f"bronze_{entity_name}").replace("\\", "/")
        silver_path = os.path.join(silver_target_dir, f"silver_{entity_name}").replace("\\", "/")
        entity_checkpoint = os.path.join(checkpoint_dir, entity_name).replace("\\", "/")

        if not os.path.exists(bronze_path) and not DeltaTable.isDeltaTable(spark, bronze_path):
            print(f"Bronze path for {entity_name} does not exist yet. Skipping stream setup.")
            continue

        print(f"Setting up Silver stream for entity: {entity_name}")
        
        # Read Bronze Delta table as a continuous stream
        bronze_stream = spark.readStream \
            .format("delta") \
            .option("ignoreChanges", "true") \
            .load(bronze_path)

        def make_batch_handler(e_name, s_path, pk):
            def batch_handler(batch_df, batch_id):
                print(f"Processing Silver micro-batch {batch_id} for entity '{e_name}'")
                batch_df.cache()
                try:
                    if e_name in SCD_TYPE_2_TABLES:
                        process_silver_scd2(spark, e_name, batch_df, s_path, pk)
                    else:
                        process_silver_scd1(spark, e_name, batch_df, s_path, pk)
                finally:
                    batch_df.unpersist()
            return batch_handler

        writer = bronze_stream.writeStream \
            .foreachBatch(make_batch_handler(entity_name, silver_path, pk_col)) \
            .option("checkpointLocation", entity_checkpoint)

        if streaming_once:
            writer.trigger(availableNow=True).start()
        else:
            writer.start()

    print("All Silver streams initialized. Awaiting termination...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
