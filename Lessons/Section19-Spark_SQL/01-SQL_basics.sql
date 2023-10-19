-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

use f1_processed

-- COMMAND ----------

show tables

-- COMMAND ----------

SELECT * FROM drivers LIMIT 10

-- COMMAND ----------

DESCRIBE drivers

-- COMMAND ----------

SELECT * 
FROM drivers
WHERE nationality = 'British'
AND dob >= '1990-01-01'

-- COMMAND ----------

SELECT name, dob as date_of_birth
FROM drivers
WHERE nationality = 'British'
AND dob >= '1990-01-01'
ORDER BY date_of_birth DESC

-- COMMAND ----------

SELECT *
FROM drivers
WHERE nationality = 'British'
AND dob >= '1990-01-01'
ORDER BY nationality, dob DESC
