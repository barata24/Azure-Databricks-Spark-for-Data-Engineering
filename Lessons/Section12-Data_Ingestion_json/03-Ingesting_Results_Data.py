# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/02-Common_Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##03-Ingesting_Results_Data

# COMMAND ----------

from pyspark.sql.types     import StructType, StructField, IntegerType, FloatType, StringType, DataType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating schema 

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId",        IntegerType(), False), \
    StructField("raceId",          IntegerType(), True), \
    StructField("driverId",        IntegerType(), True), \
    StructField("constructorId",   IntegerType(), True), \
    StructField("number",          IntegerType(), True), \
    StructField("grid",            IntegerType(), True), \
    StructField("position",        IntegerType(), True), \
    StructField("positionText",    StringType(),  True), \
    StructField("positionOrder",   StringType(),  True), \
    StructField("points",          FloatType(),   True), \
    StructField("laps",            IntegerType(), True), \
    StructField("time",            StringType(),  True), \
    StructField("milliseconds",    IntegerType(), True), \
    StructField("fastestLap",      IntegerType(), True), \
    StructField("rank",            IntegerType(), True), \
    StructField("fastestLapTime",  StringType(),  True), \
    StructField("fastestLapSpeed", FloatType(),   True), \
    StructField("statusId",        StringType(),  True)  \
])

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Reading data

# COMMAND ----------

results_df = spark.read \
                  .schema(results_schema) \
                  .json(f"{raw_folder_path}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming cols

# COMMAND ----------

results_df = results_df.withColumnRenamed("resultId",        "result_id") \
                       .withColumnRenamed("raceId",          "race_id") \
                       .withColumnRenamed("driverId",        "driver_id") \
                       .withColumnRenamed("constructorId",   "constructor_id") \
                       .withColumnRenamed("positionText",    "position_text") \
                       .withColumnRenamed("positionOrder",   "position_order") \
                       .withColumnRenamed("fastestLap",      "fastest_lap") \
                       .withColumnRenamed("fastestLapTime",  "fastest_lap_time") \
                       .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding col

# COMMAND ----------

results_df = add_ingestion_date(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding new column from widget

# COMMAND ----------

results_df = results_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Drop Col

# COMMAND ----------

results_df = results_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Write to Datalake

# COMMAND ----------

results_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl092023/processed/results"))
