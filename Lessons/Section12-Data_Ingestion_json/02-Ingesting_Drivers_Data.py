# Databricks notebook source
# MAGIC %md
# MAGIC ##01-Ingesting_Drivers_Data

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read Drivers data

# COMMAND ----------

from pyspark.sql.types     import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import current_timestamp, concat, lit

# COMMAND ----------

name_schema = StructType(fields=[ \
    StructField("forename", StringType(), True), \
    StructField("surname",  StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields=[ \
    StructField("driverId",      IntegerType(), False), \
    StructField("driverRef",     StringType(),  True), \
    StructField("number",        IntegerType(), True), \
    StructField("code",          StringType(),  True), \
    StructField("name",          name_schema), \
    StructField("dob",           DateType(),  True), \
    StructField("nationality",   StringType(),  True), \
    StructField("url",           StringType(),  True)
])

# COMMAND ----------

drivers_df = spark.read \
                  .schema(drivers_schema) \
                  .json("/mnt/formula1dl092023/raw/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming columns

# COMMAND ----------

drivers_df = drivers_df.withColumnRenamed("driverId",  "driver_id") \
                       .withColumnRenamed("driverRef", "driver_ref")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Dropping columns

# COMMAND ----------

drivers_df = drivers_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding columns

# COMMAND ----------

drivers_df = drivers_df.withColumn("name", concat("name.forename", lit(" "), "name.surname"))

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

drivers_df.write.mode("overwrite").parquet("/mnt/formula1dl092023/processed/drivers")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl092023/processed/drivers"))
