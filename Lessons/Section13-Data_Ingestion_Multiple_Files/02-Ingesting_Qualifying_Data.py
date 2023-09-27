# Databricks notebook source
# MAGIC %md
# MAGIC ##02-Ingesting_Qualifying_Data

# COMMAND ----------

# MAGIC %md
# MAGIC Note: This data that is about to be ingested is a set of multiline JSON files with the same schema and which are going to be processed simultaneously.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualifying_schema = StructType(fields=[ \
    StructField("qualifyId",     IntegerType(), False), \
    StructField("raceId",        IntegerType(), True), \
    StructField("driverId",      IntegerType(), True), \
    StructField("constructorId", IntegerType(), True), \
    StructField("number",        IntegerType(), True), \
    StructField("position",      IntegerType(), True), \
    StructField("q1",            StringType(),  True), \
    StructField("q2",            StringType(),  True), \
    StructField("q3",            StringType(),  True)  \
])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read data to Dataframe

# COMMAND ----------

qualifying_df = spark.read \
                   .schema(qualifying_schema) \
                   .option("multiLine", True) \
                   .json("/mnt/formula1dl092023/raw/qualifying/qualifying_split_*.json")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming cols

# COMMAND ----------

qualifying_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                             .withColumnRenamed("raceId", "race_id") \
                             .withColumnRenamed("constructorId", "constructor_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding cols

# COMMAND ----------

qualifying_df = qualifying_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing data to Datalake

# COMMAND ----------

qualifying_df.write.mode("overwrite").parquet("/mnt/formula1dl092023/qualifying")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl092023/qualifying"))
