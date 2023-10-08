# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/02-Common_Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##01-Ingesting_Constructors_Data

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Constructors data

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit

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
                       .json(f"{raw_folder_path}/constructors.json")

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

constructors_df = add_ingestion_date(constructors_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding new column from widget

# COMMAND ----------

constructors_df = constructors_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(constructors_df)

# COMMAND ----------

constructors_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

# COMMAND ----------

dbutils.notebook.exit("Success")
