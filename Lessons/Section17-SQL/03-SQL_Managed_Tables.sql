-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##03-SQL_Managed_Tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 1. Create Managed table using Python
-- MAGIC 2. Create Managed table using SQL
-- MAGIC 3. Effect of dropping a Managed Table
-- MAGIC 4. Describe Table

-- COMMAND ----------

-- MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

USE demo;
DESCRIBE EXTENDED race_results_python

-- COMMAND ----------

SELECT *
FROM demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

DESCRIBE TABLE EXTENDED demo.race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql

-- COMMAND ----------

SHOW TABLES IN demo
