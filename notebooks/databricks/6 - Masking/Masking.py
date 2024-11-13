# Databricks notebook source
# MAGIC %md
# MAGIC ### What are row filters?
# MAGIC Row filters allow you to apply a filter to a table so that queries return only rows that meet the filter criteria. You implement a row filter as a SQL user-defined function (UDF). Python and Scala UDFs are also supported, but only when they are wrapped in SQL UDFs.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM samples.nyctaxi.trips

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) FROM samples.nyctaxi.trips

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA catalog_excellence_school_24.dbr_exc24_giulio

# COMMAND ----------

# MAGIC
# MAGIC
# MAGIC %sql
# MAGIC CREATE TABLE catalog_excellence_school_24.dbr_exc24_giulio.trips CLONE samples.nyctaxi.trips;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION fare_amount_mask(fare_amount STRING) RETURN CASE
# MAGIC   WHEN is_member('admins') THEN fare_amount
# MAGIC   ELSE '**.**'
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE catalog_excellence_school_24.dbr_exc24_giulio.trips SET ROW FILTER fare_amount_mask ON (fare_amount);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM catalog_excellence_school_24.dbr_exc24_giulio.trips

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE catalog_excellence_school_24.dbr_exc24_giulio.trips DROP ROW FILTER;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP FUNCTION fare_amount_mask_denied

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION fare_amount_mask_denied(fare_amount double) RETURN CASE
# MAGIC   WHEN is_member('pippo') THEN TRUE
# MAGIC   ELSE fare_amount = 0
# MAGIC END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE catalog_excellence_school_24.dbr_exc24_giulio.trips SET ROW FILTER fare_amount_mask_denied ON (fare_amount);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM catalog_excellence_school_24.dbr_exc24_giulio.trips

# COMMAND ----------

# MAGIC %md
# MAGIC ### What are column masks?
# MAGIC Column masks let you apply a masking function to a table column. The masking function evaluates at query runtime, substituting each reference of the target column with the results of the masking function. For most use cases, column masks determine whether to return the original column value or redact it based on the identity of the invoking user. Column masks are expressions written as SQL UDFs or as Python or Scala UDFs that are wrapped in SQL UDFs.
# MAGIC
# MAGIC Each table column can have only one masking function applied to it. The masking function takes the unmasked value of the column as input and returns the masked value as its result. The return value of the masking function should be the same type as the column being masked. The masking function can also take additional columns as input parameters and use them in its masking logic.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply a column mask
# MAGIC To apply a column mask, create a function (UDF) and then apply it to a table column.
# MAGIC
# MAGIC You can apply a column mask using Catalog Explorer or SQL commands. The Catalog Explorer instructions assume that you have already created a function and that it is registered in Unity Catalog. The SQL instructions include examples of creating a column mask function and applying it to a table column.
# MAGIC
# MAGIC Catalog Explorer
# MAGIC SQL
# MAGIC In your Databricks workspace, click Catalog icon Catalog.
# MAGIC
# MAGIC Browse or search for the table.
# MAGIC
# MAGIC On the Overview tab, find the row you want to apply the column mask to and click the Edit icon Mask edit icon.
# MAGIC
# MAGIC On the Add column mask dialog, select the catalog and schema that contain the filter function, then select the function.
# MAGIC
# MAGIC On the expanded dialog, view the function definition. If the function includes any parameters in addition to the column that is being masked, select the table columns that you want to cast those additional function parameters to.
# MAGIC
# MAGIC Click Add.
# MAGIC
# MAGIC To remove the column mask from the table, click fx Column mask in the table row and click Remove.