-- Databricks notebook source
use f1_processed

-- COMMAND ----------

select *, concat(driver_ref, '-', code) AS new_driver_ref
from drivers

-- COMMAND ----------

select *, split(name, ' ')[0] forename, split(name, ' ')[1] surname 
from drivers 

-- COMMAND ----------

select *, date_format(dob, 'dd-MM-yyyy')
from drivers

-- COMMAND ----------

select *, date_add(dob, 1)
from drivers

-- COMMAND ----------

select count(*)
from drivers

-- COMMAND ----------

select max(dob)
from drivers

-- COMMAND ----------

select nationality, count(*) as driversFromEachNat
from drivers
group by nationality
order by nationality

-- COMMAND ----------

select nationality, count(*) as driversFromEachNat
from drivers
group by nationality
having count(*) > 100

-- COMMAND ----------

SELECT nationality, name, dob, rank() OVER (PARTITION BY nationality ORDER BY dob DESC) AS age_Rank
FROM drivers
ORDER BY nationality, age_Rank

-- COMMAND ----------


