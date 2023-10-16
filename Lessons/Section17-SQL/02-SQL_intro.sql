-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##02-SQL_intro

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Objectives:
-- MAGIC 1. Spark SQL Documentation
-- MAGIC 2. Creattabase demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. **SHOW** command
-- MAGIC 5. **DESCRIBE** command
-- MAGIC 6. Find the current Database

-- COMMAND ----------

CREATE DATABASE demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES

-- COMMAND ----------

DESCRIBE DATABASE DEMO

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

USE demo

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES
