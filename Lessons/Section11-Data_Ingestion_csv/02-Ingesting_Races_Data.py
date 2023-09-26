# Databricks notebook source
# MAGIC %md
# MAGIC ##02-Ingesting_Races_Data

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read races data

# COMMAND ----------

from pyspark.sql.types     import StructType, StructField, IntegerType, DoubleType, StringType, DateType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, lit

# COMMAND ----------

races_schema = StructType(fields=[ \
    StructField("raceId",    IntegerType(), False), \
    StructField("year",      IntegerType(), True), \
    StructField("round",     IntegerType(), True), \
    StructField("circuitId", IntegerType(), True), \
    StructField("name",      StringType(),  True), \
    StructField("date",      DateType(),    True), \
    StructField("time",      StringType(),  True), \
    StructField("url",       StringType(),  True)
])

# COMMAND ----------

races_df = spark.read \
                .option("header", True) \
                .schema(races_schema) \
                .csv("/mnt/formula1dl092023/raw/races.csv")


# COMMAND ----------

display(races_df.printSchema())

# COMMAND ----------

display(races_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming columns

# COMMAND ----------

races_df = races_df.withColumnRenamed("raceId", "race_id") \
                   .withColumnRenamed("year", "race_year") \
                   .withColumnRenamed("circuitId", "circuit_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Dropping columns

# COMMAND ----------

races_df = races_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding columns

# COMMAND ----------

races_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                   .withColumn("race_timestamp", to_timestamp(concat(races_df.date, lit(" "), races_df.time), "yyyy-MM-dd HH:mm:ss"))

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing output to Datalake

# COMMAND ----------

races_df.write.mode("overwrite").partitionBy("race_year").parquet("/mnt/formula1dl092023/processed/races")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl092023/processed/"))

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl092023/processed/races"))
