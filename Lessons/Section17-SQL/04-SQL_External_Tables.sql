-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##04-SQL_External_Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_ext_py

-- COMMAND ----------

-- MAGIC %md
-- MAGIC The table created below will **appear in the Hive Metastore even when the table is empty**, but **it won't in the Azure Data Lake**.

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(
  race_year int,
  race_name string,
  race_date string,
  circuit_location string,
  driver_name string, 
  driver_number string,
  driver_nationality STRING,
  team string,
  grid int,
  fastest_lap_time string,
  race_time STRING,
  points float,
  position int,
  created_date timestamp
)
USING parquet 
LOCATION "/mnt/formula1dl102023/presentation/race_results_ext_sql"

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_ext_sql

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py 
WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM demo.race_results_ext_sql

-- COMMAND ----------

SELECT COUNT(*) FROM demo.race_results_ext_sql

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Dropping the external table

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####The table will not appear in the Hive Metastore because the metadata of this data is deleted, but the actual data is still located in the **EXTERNAL LOCATION** - ADLS.

-- COMMAND ----------

SHOW TABLES IN demo
