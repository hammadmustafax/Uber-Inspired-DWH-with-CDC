import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sha2, concat_ws, current_timestamp, when, lit
from pyspark.sql.types import StructType, StructField, StringType, LongType
# pyrefly: ignore [missing-import]
from delta import configure_spark_with_delta_pip

def create_spark_session():
    """Initializes the Spark session with Delta Lake and Kafka support."""
    builder = SparkSession.builder \
        .appName("UberBronzeIngestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    
    # Configure Spark session with Delta packages
    return configure_spark_with_delta_pip(builder).getOrCreate()

# Define Debezium Kafka JSON schema structures
# Debezium wraps data in a 'schema' block and a 'payload' block when schemas.enable=true
source_schema = StructType([
    StructField("version", StringType(), True),
    StructField("connector", StringType(), True),
    StructField("name", StringType(), True),
    StructField("ts_ms", LongType(), True),
    StructField("snapshot", StringType(), True),
    StructField("db", StringType(), True),
    StructField("sequence", StringType(), True),
    StructField("schema", StringType(), True),
    StructField("table", StringType(), True),
    StructField("txId", LongType(), True),
    StructField("lsn", LongType(), True),
    StructField("xmin", LongType(), True)
])

debezium_payload_schema = StructType([
    StructField("before", StringType(), True),
    StructField("after", StringType(), True),
    StructField("source", source_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True)
])

envelope_schema = StructType([
    StructField("schema", StringType(), True),
    StructField("payload", debezium_payload_schema, True),
    StructField("_corrupt_record", StringType(), True)
])

def main():
    # Load configuration from environment variables
    kafka_brokers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
    bronze_target_dir = os.getenv("BRONZE_TARGET_DIR", "/app/data/delta/bronze")
    checkpoint_dir = os.getenv("CHECKPOINT_DIR", "/app/data/delta/checkpoints/bronze")
    streaming_once = os.getenv("STREAMING_ONCE", "false").lower() == "true"
    dry_run = "--dry-run" in sys.argv

    print("Starting Bronze Ingestion Layer...")
    print(f"Kafka Brokers: {kafka_brokers}")
    print(f"Bronze Target Path: {bronze_target_dir}")
    print(f"Checkpoint Location: {checkpoint_dir}")
    print(f"Streaming Mode: {'Trigger AvailableNow (Batch)' if streaming_once else 'Continuous'}")

    spark = create_spark_session()

    if dry_run:
        print("Dry run requested. Spark session initialized successfully. Exiting.")
        spark.stop()
        sys.exit(0)

    # Subscribe to all Debezium CDC topics using pattern match
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_brokers) \
        .option("subscribePattern", "uber_cdc.public.*") \
        .option("startingOffsets", "earliest") \
        .load()

    # Convert binary key and value to string
    df_str = df.selectExpr(
        "CAST(key AS STRING) as key_str",
        "CAST(value AS STRING) as value_str",
        "topic",
        "partition",
        "offset",
        "timestamp"
    )

    def write_to_bronze_and_dlq(batch_df, batch_id):
        print(f"Processing micro-batch {batch_id}")
        
        # Cache batch dataframe since it will be queried multiple times
        batch_df.cache()
        try:
            # Parse the Debezium JSON value envelope
            parsed_batch = batch_df.withColumn("parsed_value", from_json(col("value_str"), envelope_schema))

            # Define logical criteria for data corruption:
            # - value is not null but parsed payload is missing
            # - OR _corrupt_record is populated by Spark's JSON parser
            is_corrupt = (
                col("value_str").isNotNull() & 
                (col("parsed_value").isNull() | col("parsed_value.payload").isNull())
            ) | col("parsed_value._corrupt_record").isNotNull()

            # 1. Handle Corrupt Records (Dead-Letter Queue)
            corrupt_records = parsed_batch.filter(is_corrupt)
            if corrupt_records.count() > 0:
                print(f"Found {corrupt_records.count()} corrupted records. Writing to DLQ.")
                dlq_df = corrupt_records.select(
                    col("topic"),
                    col("partition"),
                    col("offset"),
                    col("timestamp").alias("kafka_timestamp"),
                    col("key_str").alias("key"),
                    col("value_str").alias("value"),
                    current_timestamp().alias("_ingested_at"),
                    when(col("parsed_value._corrupt_record").isNotNull(), col("parsed_value._corrupt_record"))
                    .otherwise(lit("Parsing failed: missing payload or invalid structure")).alias("error_message")
                )
                
                dlq_path = os.path.join(bronze_target_dir, "bronze_corrupted_records")
                dlq_df.write \
                    .format("delta") \
                    .mode("append") \
                    .option("mergeSchema", "true") \
                    .save(dlq_path)

            # 2. Handle Clean Records
            clean_records = parsed_batch.filter(~is_corrupt)
            if clean_records.count() > 0:
                print(f"Found {clean_records.count()} clean records. Processing CDC logic.")
                payload = col("parsed_value.payload")
                
                # Generate a deterministic CDC event hash
                # SHA256(concat_ws('|', value.source.lsn, value.source.txId, value.source.table, key, value.op))
                event_hash_col = sha2(
                    concat_ws(
                        '|',
                        payload.getField("source").getField("lsn"),
                        payload.getField("source").getField("txId"),
                        payload.getField("source").getField("table"),
                        col("key_str"),
                        payload.getField("op")
                    ),
                    256
                )

                final_clean_df = clean_records.select(
                    event_hash_col.alias("_cdc_event_hash"),
                    col("topic"),
                    col("partition"),
                    col("offset"),
                    col("timestamp").alias("kafka_timestamp"),
                    col("key_str").alias("key"),
                    col("value_str").alias("value"),
                    col("parsed_value.payload.before").alias("before"),
                    col("parsed_value.payload.after").alias("after"),
                    col("parsed_value.payload.op").alias("op"),
                    col("parsed_value.payload.source.lsn").alias("lsn"),
                    col("parsed_value.payload.source.txId").alias("txId"),
                    col("parsed_value.payload.source.ts_ms").alias("source_ts_ms"),
                    current_timestamp().alias("_ingested_at")
                )

                # Group by topic to append into separate Delta tables
                distinct_topics = [row.topic for row in final_clean_df.select("topic").distinct().collect()]
                for topic in distinct_topics:
                    table_name = topic.split(".")[-1]
                    topic_df = final_clean_df.filter(col("topic") == topic)
                    
                    target_path = os.path.join(bronze_target_dir, f"bronze_{table_name}")
                    print(f"Appending {topic_df.count()} records to {target_path}")
                    topic_df.write \
                        .format("delta") \
                        .mode("append") \
                        .option("mergeSchema", "true") \
                        .save(target_path)
            else:
                print("No clean records found in this micro-batch.")
        finally:
            # Clean up cache
            batch_df.unpersist()

    # Configure query and output stream
    writer = df_str.writeStream \
        .foreachBatch(write_to_bronze_and_dlq) \
        .option("checkpointLocation", checkpoint_dir)

    if streaming_once:
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
