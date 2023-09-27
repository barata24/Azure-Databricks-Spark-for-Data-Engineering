# Databricks notebook source
# MAGIC %md
# MAGIC ##01-Ingesting_Laptimes_Data

# COMMAND ----------

# MAGIC %md
# MAGIC Note: This data that is about to be ingested is a set of CSV files with the same schema and which are going to be processed simultaneously.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

laptimes_schema = StructType(fields=[ \
    StructField("raceId",       IntegerType(), False), \
    StructField("driverId",     IntegerType(), True), \
    StructField("lap",          IntegerType(), True), \
    StructField("position",     IntegerType(), True), \
    StructField("time",         StringType(),  True), \
    StructField("milliseconds", IntegerType(), True)  \
])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read data to Dataframe

# COMMAND ----------

laptimes_df = spark.read \
                   .schema(laptimes_schema) \
                   .csv("/mnt/formula1dl092023/raw/lap_times/lap_times_split*.csv")

# COMMAND ----------

display(laptimes_df)

# COMMAND ----------

laptimes_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming cols

# COMMAND ----------

laptimes_df = laptimes_df.withColumnRenamed("raceId", "race_id") \
                         .withColumnRenamed("driverId", "driver_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding cols

# COMMAND ----------

laptimes_df = laptimes_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing data to Datalake

# COMMAND ----------

laptimes_df.write.mode("overwrite").parquet("/mnt/formula1dl092023/lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl092023/lap_times"))
