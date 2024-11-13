# Databricks notebook source
# MAGIC %md
# MAGIC ### Streaming Types
# MAGIC A query on the input generates a result table. At every trigger interval (say, every 1 second), new rows are appended to the input table, which eventually updates the result table. Whenever the result table is updated, the changed result rows are written to an external sink. The output is defined as what gets written to external storage. The output can be configured in different modes:
# MAGIC
# MAGIC - Complete Mode: The entire updated result table is written to external storage. It is up to the storage connector to decide how to handle the writing of the entire table.
# MAGIC - Append Mode: Only new rows appended in the result table since the last trigger are written to external storage. This is applicable only for the queries where existing rows in the Result Table are not expected to change.
# MAGIC - Update Mode: Only the rows that were updated in the result table since the last trigger are written to external storage. This is different from Complete Mode in that Update Mode outputs only the rows that have changed since the last trigger. If the query doesn't contain aggregations, it is equivalent to Append mode.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load sample data
# MAGIC The easiest way to get started with Structured Streaming is to use an example Databricks dataset available in the /databricks-datasetsfolder accessible within the Databricks workspace. Databricks has sample event data as files in /databricks-datasets/structured-streaming/events/to use to build a Structured Streaming application. Let's take a look at the contents of this directory.

# COMMAND ----------

# MAGIC %fs ls /databricks-datasets/structured-streaming/events/

# COMMAND ----------

# MAGIC %md
# MAGIC ### Initialize the stream
# MAGIC Since the sample data is just a static set of files, you can emulate a stream from them by reading one file at a time, in the chronological order in which they were created.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, TimestampType, StringType
from pyspark.sql.functions import window

# COMMAND ----------

inputPath = "/databricks-datasets/structured-streaming/events/"

# COMMAND ----------

# Define the schema to speed up processing
jsonSchema = StructType([ StructField("time", TimestampType(), True), StructField("action", StringType(), True) ])

# COMMAND ----------

streamingInputDF = (
  spark
    .readStream
    .schema(jsonSchema)               # Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  # Treat a sequence of files as a stream by picking one file at a time
    .json(inputPath)
)

# COMMAND ----------

streamingCountsDF = (
  streamingInputDF
    .groupBy(
      streamingInputDF.action,
      window(streamingInputDF.time, "1 hour"))
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Start the streaming job
# MAGIC You start a streaming computation by defining a sink and starting it. In our case, to query the counts interactively, set the completeset of 1 hour counts to be in an in-memory table.

# COMMAND ----------

query = (
  streamingCountsDF
    .writeStream
    .format("memory")        # memory = store in-memory table (for testing only)
    .queryName("counts")     # counts = name of the in-memory table
    .outputMode("complete")  # complete = all the counts should be in the table
    .start()
)

# COMMAND ----------

query.id

# COMMAND ----------

query.status

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interactively query the stream
# MAGIC We can periodically query the countsaggregation:

# COMMAND ----------

# MAGIC %sql 
# MAGIC select action, date_format(window.end, "MMM-dd HH:mm") as time, count from counts order by time, action