-- Create OLTP database and Airflow backend database
CREATE DATABASE uber_oltp;
CREATE DATABASE airflow_db;

-- Grant permissions
GRANT ALL PRIVILEGES ON DATABASE uber_oltp TO postgres;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO postgres;
