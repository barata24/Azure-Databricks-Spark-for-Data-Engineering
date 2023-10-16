-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Let's handle csv files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create **circuits table** from circuits.csv file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits(
  circuitId   Int,
  circuitRef  String,  
  name        String,  
  location    String,  
  country     String,  
  lat         Double,  
  lng         Double,  
  alt         Int, 
  url         String
)
USING CSV
OPTIONS(path "/mnt/formula1dl102023/raw/circuits.csv",
        header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create **races** table from races.csv file

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races(
  raceId    Int,
  year      Int,
  round     Int,
  circuitId Int,
  name      String,
  date      Date,
  time      String,
  url       String
)
USING CSV
OPTIONS(path "/mnt/formula1dl102023/raw/races.csv",
        header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####Let's handle JSON files

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create constructors table
-- MAGIC - Single line JSON
-- MAGIC - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors(
  constructorId  INT,
  constructorRef String,
  name           String,
  nationality    String,
  url            String
)
USING JSON
OPTIONS(path "/mnt/formula1dl102023/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create drivers table
-- MAGIC - Single line JSON
-- MAGIC - More Complex Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers(
driverId INT,
driverRef STRING,
number INT,
code String,
name STRUCT<forename: STRING, surname: String>,
dob DATE,
nationality STRING,
url STRING
)
USING JSON
OPTIONS(path "/mnt/formula1dl102023/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create results table
-- MAGIC - Single line JSON
-- MAGIC - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results(
    resultId        Int,
    raceId          Int,
    driverId        Int,
    constructorId   Int,
    number          Int,
    grid            Int,
    position        Int,
    positionText    String,
    positionOrder   Int,
    points          Int,
    laps            Int,
    time            String,
    milliseconds    Int,
    fastestLap      Int,
    rank            Int,
    fastestLapTime  String,
    fastestLapSpeed Float,
    statusId        String
)
USING JSON
OPTIONS(path "/mnt/formula1dl102023/raw/results.json")

-- COMMAND ----------

SELECT * FROM f1_raw.results

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Create pitstops table
-- MAGIC - Multi Line JSON
-- MAGIC - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops(
    raceId      Int,
    driverId    Int,
    stop        Int,
    lap         Int,
    time        String,
    duration    String,
    milliseconds Int
)
USING JSON
OPTIONS(path "/mnt/formula1dl102023/raw/pit_stops.json",
        multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops
