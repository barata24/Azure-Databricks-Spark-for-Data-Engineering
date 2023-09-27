# Databricks notebook source
# MAGIC %md
# MAGIC ####04-Ingesting_Pitstops_Data

# COMMAND ----------

# MAGIC %md
# MAGIC Note: This JSON file is not as the previous ones since this has multiline JSON objects instead of single line objects 

# COMMAND ----------

from pyspark.sql.types     import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

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
                   .json("/mnt/formula1dl092023/raw/pit_stops.json")

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

pitstops_df = pitstops_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing data to Datalake

# COMMAND ----------

pitstops_df.write.mode("overwrite").parquet("/mnt/formula1dl092023/processed/pit_stops")
