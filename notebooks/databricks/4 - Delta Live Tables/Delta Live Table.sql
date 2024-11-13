-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Delta Live Table
-- MAGIC Delta Live Tables is a declarative framework for building reliable, maintainable, and testable data processing pipelines. You define the transformations to perform on your data and Delta Live Tables manages task orchestration, cluster management, monitoring, data quality, and error handling.
-- MAGIC
-- MAGIC Instead of defining your data pipelines using a series of separate Apache Spark tasks, you define streaming tables and materialized views that the system should create and keep up to date. Delta Live Tables manages how your data is transformed based on queries you define for each processing step. You can also enforce data quality with Delta Live Tables expectations, which allow you to define expected data quality and specify how to handle records that fail those expectations.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create Table

-- COMMAND ----------

CREATE TABLE catalog_excellence_school_24.dbr_exc24_giulio.people_delta_live_table (
  id INT NOT NULL,
  firstName STRING,
  middleName STRING NOT NULL,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Validate & Alter Checks

-- COMMAND ----------

INSERT INTO catalog_excellence_school_24.dbr_exc24_giulio.people_delta_live_table VALUES
(1, "Giulio", NULL, "Melloni", "Male", 1994-02-14, NULL, NULL)

-- COMMAND ----------

ALTER TABLE catalog_excellence_school_24.dbr_exc24_giulio.people_delta_live_table ALTER COLUMN middleName DROP NOT NULL;

-- COMMAND ----------

INSERT INTO catalog_excellence_school_24.dbr_exc24_giulio.people_delta_live_table VALUES
(1, "Giulio", NULL, "Melloni", "Male", 1994-02-14, NULL, NULL)

-- COMMAND ----------

ALTER TABLE catalog_excellence_school_24.dbr_exc24_giulio.people_delta_live_table ALTER COLUMN ssn SET NOT NULL;

-- COMMAND ----------

DELETE FROM catalog_excellence_school_24.dbr_exc24_giulio.people_delta_live_table
WHERE id=1

-- COMMAND ----------

ALTER TABLE catalog_excellence_school_24.dbr_exc24_giulio.people_delta_live_table ALTER COLUMN ssn SET NOT NULL;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Custom Constraints

-- COMMAND ----------

ALTER TABLE catalog_excellence_school_24.dbr_exc24_giulio.people_delta_live_table ADD CONSTRAINT dateWithinRange CHECK (birthDate > '1900-01-01');

-- COMMAND ----------

INSERT INTO catalog_excellence_school_24.dbr_exc24_giulio.people_delta_live_table VALUES
(1, "Giulio", NULL, "Melloni", "Male", 1492-10-12, NULL, NULL)

-- COMMAND ----------

ALTER TABLE catalog_excellence_school_24.dbr_exc24_giulio.people_delta_live_table DROP CONSTRAINT dateWithinRange;

-- COMMAND ----------

ALTER TABLE catalog_excellence_school_24.dbr_exc24_giulio.people_delta_live_table ADD CONSTRAINT validIds CHECK (id > 1 and id < 99999999);

-- COMMAND ----------

DESCRIBE DETAIL catalog_excellence_school_24.dbr_exc24_giulio.people_delta_live_table;

-- COMMAND ----------

SHOW TBLPROPERTIES catalog_excellence_school_24.dbr_exc24_giulio.people_delta_live_table;