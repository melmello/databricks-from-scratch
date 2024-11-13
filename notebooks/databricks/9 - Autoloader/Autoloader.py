# Databricks notebook source
# MAGIC %md
# MAGIC ### Load sample data
# MAGIC The easiest way to get started with Structured Streaming is to use an example Databricks dataset available in the /databricks-datasetsfolder accessible within the Databricks workspace. Databricks has sample event data as files in /databricks-datasets/structured-streaming/events/to use to build a Structured Streaming application. Let's take a look at the contents of this directory.

# COMMAND ----------

# MAGIC %fs cp -r /databricks-datasets/structured-streaming/events dbfs:/tmp/events

# COMMAND ----------

# MAGIC %fs ls /tmp/events
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Use Auto Loader to read streaming data from object storage
# MAGIC The following example demonstrates loading JSON data with Auto Loader, which uses cloudFiles to denote format and options. The schemaLocation option enables schema inference and evolution. Paste the following code in a Databricks notebook cell and run the cell to create a streaming DataFrame named raw_df:

# COMMAND ----------

file_path = "/tmp/events"
checkpoint_path = "/tmp/ss-tutorial/_checkpoint"

raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint_path)
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .load(file_path)
)

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

transformed_df = (raw_df.select(
    "*",
    col("_metadata.file_path").alias("source_file"),
    current_timestamp().alias("processing_time")
    )
)

# COMMAND ----------

query = (transformed_df.writeStream
    .format("memory")
    .queryName("counts")
    .start()
)

# COMMAND ----------

# MAGIC %fs cp /tmp/events/file-0.json dbfs:/tmp/events/file-tmp-0.json

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM counts

# COMMAND ----------

# MAGIC %md
# MAGIC Like other read operations on Databricks, configuring a streaming read does not actually load data. You must trigger an action on the data before the stream begins.__