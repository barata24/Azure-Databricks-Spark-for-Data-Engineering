# Databricks notebook source
# MAGIC %md
# MAGIC ##01-Ingesting_Constructors_Data

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Constructors data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

#Below we'll have two distinct ways to create our schema:
# 1 - Using StructTypes
# 2 - Inferring it directly as a String

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_schema = StructType(fields=[ \
    StructField("constructorId",  IntegerType(), False), \
    StructField("constructorRef", StringType(), True), \
    StructField("name",           StringType(), True), \
    StructField("nationality",    StringType(), True), \
    StructField("url",            StringType(), True)    
])

# COMMAND ----------

constructors_df = spark.read \
                       .schema(constructors_schema) \
                       .json("/mnt/formula1dl092023/raw/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming columns

# COMMAND ----------

constructors_df.withColumnRenamed("constructorId", "constructor_id") \
               .withColumnRenamed("constructorRef", "constructor_ref")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Dropping columns

# COMMAND ----------

constructors_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding columns

# COMMAND ----------

constructors_df = constructors_df.withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_df.write.mode("overwrite").parquet("/mnt/formula1dl092023/processed/constructors")
