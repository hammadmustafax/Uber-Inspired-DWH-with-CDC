FROM apache/spark:3.5.1

USER root

# Create home directory for spark user and configure write permissions for Ivy cache
RUN mkdir -p /home/spark/.ivy2/cache && chown -R spark /home/spark

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    delta-spark==3.0.0 \
    soda-core-spark==3.0.45 \
    psycopg2-binary==2.9.9 \
    python-dotenv==1.0.1

# Switch back to the default spark user for running processes and caching jars
USER spark

# Pre-fetch Spark Kafka Integration and Delta Lake jars
RUN spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.0.0 --exec "exit()"

# Set working directory
WORKDIR /app
