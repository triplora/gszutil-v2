CREATE TABLE `[PROJECT_ID].[DATASET_NAME].[TABLE_NAME]` (
   job_name STRING,
   job_date DATE,
   job_time TIME,
   timestamp TIMESTAMP,
   job_id STRING,
   job_type STRING,
   source STRING,
   destination STRING,
   job_json STRING,
   records_in INT64,
   records_out INT64,
   inserted INT64,
   updated INT64
)
PARTITION BY job_date
CLUSTER BY job_name, job_date, timestamp
OPTIONS (
  partition_expiration_days=1000,
  description="Log table for mainframe jobs"
