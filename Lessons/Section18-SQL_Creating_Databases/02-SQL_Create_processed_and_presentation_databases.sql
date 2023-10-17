-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###Create processed Database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/formula1dl102023/processed"

-- COMMAND ----------

DESCRIBE DATABASE f1_processed

-- COMMAND ----------

-- MAGIC %md
-- MAGIC So that we can create our Managed Tables, we're going to use the already created Ingestion Notebooks that use PySpark but we're going the tweak a bit the write command to create the tables in the desired Database - **f1_processed**. <br>
-- MAGIC These write commands were used to write our data to parquet files, but now we also want to have this data in the Database mentioned. <br> 
-- MAGIC To achieve this, we're using the **.saveAsTable** method.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create presentation Database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/formula1dl102023/presentation"
