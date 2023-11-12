-- Databricks notebook source
USE f1_processed

-- COMMAND ----------

CREATE TABLE f1_presentation.calculated_race_results
USING PARQUET
AS
SELECT ra.race_year,
       c.name constructor_name,
       d.name driver_name,
       re.position,
       re.points,
       11 - re.position AS calculated_points
FROM f1_processed.results            AS re
INNER JOIN f1_processed.drivers      AS d  ON re.driver_id      = d.driver_id
INNER JOIN f1_processed.constructors AS c  ON re.constructor_id = c.constructorId
INNER JOIN f1_processed.races        AS ra ON re.race_id        = ra.race_id
WHERE re.position <= 10

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results

-- COMMAND ----------



-- COMMAND ----------


