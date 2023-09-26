# Databricks notebook source
# MAGIC %md
# MAGIC ##01-Ingesting_Circuits_Data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql.functions import col, current_timestamp

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl092023/raw"))

# COMMAND ----------

#StructField("field_name", DataType, is_nullable)
circuits_schema = StructType(fields=[
    StructField("circuitId",   IntegerType(), False),
    StructField("circuitRef",  StringType(),  True),
    StructField("name",        StringType(),  True),
    StructField("location",    StringType(),  True),
    StructField("country",     StringType(),  True),
    StructField("lat",         DoubleType(),  True),
    StructField("lng",         DoubleType(),  True),
    StructField("alt",         IntegerType(), True),
    StructField("url",         StringType(),  True)
])

# COMMAND ----------

circuits_df = spark.read \
                  .option("header", True) \
                  .schema(circuits_schema) \
                  .csv("/mnt/formula1dl092023/raw/circuits.csv")

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

circuits_df.printSchema()

# COMMAND ----------

display(circuits_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming columns

# COMMAND ----------

circuitsDF = circuits_df.withColumnRenamed("circuitsId", "circuit_id") \
                       .withColumnRenamed("circuitRed", "circuit_ref") \
                       .withColumnRenamed("lat", "latitude") \
                       .withColumnRenamed("long", "longitude")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Dropping "url" column

# COMMAND ----------

circuits_df.columns

# COMMAND ----------

# "Drop" by not selecting a specific column
circuits_df_selected = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")

# COMMAND ----------

#Alternative 2
circuits_df_selected = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)

# COMMAND ----------

#Alternative 3
circuits_df_selected = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

#Alternative 4
circuits_df_selected = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# Actually dropping a column
circuits_df = circuits_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding new column

# COMMAND ----------

circuits_df = circuitsDF.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write our data to Datalake as parquet

# COMMAND ----------

circuitsDF.write.mode("overwrite").parquet("/mnt/formula1dl092023/processed/circuits")

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/formula1dl092023/processed/circuits

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl092023/processed/circuits"))

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl092023/processed/circuits"))
