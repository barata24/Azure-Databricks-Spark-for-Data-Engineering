-- Databricks notebook source
USE f1_presentation

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT race_year, driver_name, team_name, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2018

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT race_year, driver_name, team_name, total_points, wins, rank
FROM driver_standings
WHERE race_year = 2020

-- COMMAND ----------

select * from v_driver_standings_2018

-- COMMAND ----------

--Drivers who were in both races
select a.*, b.*
from       v_driver_standings_2018 a
inner join v_driver_standings_2020 b
ON a.driver_name = b.driver_name

-- COMMAND ----------

--Drivers who were in 2018 but were not in 2020
select a.*, b.*
from       v_driver_standings_2018 a
left join  v_driver_standings_2020 b
ON a.driver_name = b.driver_name

-- COMMAND ----------

--Drivers who were in either of the 2 races
select a.*, b.*
from        v_driver_standings_2018 a
full join  v_driver_standings_2020 b
ON a.driver_name = b.driver_name

-- COMMAND ----------

--Same as inner join but only receiving data from table a
select *
from        v_driver_standings_2018 a
semi join   v_driver_standings_2020 b
ON a.driver_name = b.driver_name

-- COMMAND ----------

--Opposite of inner join
select *
from        v_driver_standings_2018 a
anti join   v_driver_standings_2020 b
ON a.driver_name = b.driver_name

-- COMMAND ----------

--cartesian product for every record in a table with the every other record from the other table
select *
from        v_driver_standings_2018 a
cross join   v_driver_standings_2020 b
ON a.driver_name = b.driver_name
