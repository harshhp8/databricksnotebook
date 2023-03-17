# Databricks notebook source
print("hello world")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE sampletable;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE sampletable SET TBLPROPERTIES('delta.minReaderVersion' = '2', 'delta.minWriterVersion' = '5')

# COMMAND ----------

def autoload_to_table(data_source, source_format, table_name, checkpoint_directory):
    query = (spark.readStream
                  .format("cloudFiles")
                  .option("cloudFiles.format", source_format)
                  .option("cloudFiles.schemaLocation", checkpoint_directory)                   
                  .load(data_source)
                  .writeStream
                  .option("checkpointLocation", checkpoint_directory)
                  .option("mergeSchema", "true")
                  .table(table_name))
    return query

# COMMAND ----------

query = autoload_to_table(data_source = "s3://databricksbucketforharsh/",
                          source_format = "csv",
                          table_name = "target_table",
                          checkpoint_directory = "/")

# COMMAND ----------

aws_bucket_name = "databricksbucketforharsh/"
mount_name = "s3mount"
dbutils.fs.mount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

dbutils.fs.refreshMounts()

# COMMAND ----------

display(dbutils.fs.ls("/mnt/s3mount"))

# COMMAND ----------

dbutils.fs.unmount("/mnt/s3mount")

# COMMAND ----------

dbutils.fs.mv("/mnt/s3mount","/harsh",recurse= True)

# COMMAND ----------

dbutils.fs.help("mv")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

# Import functions
from pyspark.sql.functions import input_file_name, current_timestamp

# Define variables used in code below
#file_path = "s3://databricksbucketforharsh/"
file_path = "/mnt/s3mount"
username = "harsh"
table_name = f"{username}_etl_quickstart"
checkpoint_path = f"/tmp/{username}/_checkpoint/etl_quickstart"

# Clear out data from previous demo execution
spark.sql(f"DROP TABLE IF EXISTS {table_name}")
dbutils.fs.rm(checkpoint_path, True)

# Configure Auto Loader to ingest JSON data to a Delta table
(spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("cloudFiles.schemaLocation", checkpoint_path)
  .load(file_path)
  .select("*", input_file_name().alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
  .option("checkpointLocation", checkpoint_path)
  .trigger(availableNow=True)
  .toTable(table_name))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from harsh_etl_quickstart where Zone_Name='West';

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY harsh_etl_quickstart

# COMMAND ----------

for s in spark.streams.active:
    print("Stopping " + s.id)
    s.stop()
    s.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gaurangtab.gold_tab;
