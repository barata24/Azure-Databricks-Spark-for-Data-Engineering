-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##05-SQL_Views

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Views on tables
-- MAGIC ##### Learning objectives
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permanent View

-- COMMAND ----------

-- MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2018

-- COMMAND ----------

SELECT * FROM v_race_results

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2012

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Don't forget to access the "global_temp" to access the global temp view.

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create a permanent View

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *
FROM demo.race_results_python
WHERE race_year = 2012

-- COMMAND ----------

SHOW TABLES IN demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Try to detach and re-attach the cluster and execute the following cell.

-- COMMAND ----------

SELECT *
FROM demo.pv_race_results
