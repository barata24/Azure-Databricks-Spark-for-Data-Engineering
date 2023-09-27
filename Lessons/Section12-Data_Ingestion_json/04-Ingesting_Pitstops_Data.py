# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/02-Common_Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ####04-Ingesting_Pitstops_Data

# COMMAND ----------

# MAGIC %md
# MAGIC Note: This JSON file is not as the previous ones since this has multiline JSON objects instead of single line objects 

# COMMAND ----------

from pyspark.sql.types     import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pitstops_schema = StructType(fields=[ \
    StructField("raceId",      IntegerType(), False), \
    StructField("driverId",    IntegerType(), True), \
    StructField("stop",        StringType(),  True), \
    StructField("lap",         IntegerType(), True), \
    StructField("time",        StringType(),  True), \
    StructField("duration",    StringType(),  True), \
    StructField("milliseconds", IntegerType(), True)  \
])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read data to Dataframe

# COMMAND ----------

pitstops_df = spark.read \
                   .schema(pitstops_schema) \
                   .option("multiLine", True) \
                   .json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(pitstops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming cols

# COMMAND ----------

pitstops_df = pitstops_df.withColumnRenamed("raceId", "race_id") \
                         .withColumnRenamed("driverId", "driver_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding cols

# COMMAND ----------

pitstops_df = add_ingestion_date(pitstops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding new column from widget

# COMMAND ----------

pitstops_df = pitstops_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing data to Datalake

# COMMAND ----------

pitstops_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")
