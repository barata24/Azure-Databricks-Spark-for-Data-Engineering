# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-3-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/02-Common_Functions"

# COMMAND ----------

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
                  .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

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

drivers_df = add_ingestion_date(drivers_df)

# COMMAND ----------

drivers_df = drivers_df.withColumn("name", concat("name.forename", lit(" "), "name.surname")) \
                       .withColumn("file_date", v_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding new column from widget

# COMMAND ----------

drivers_df = drivers_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

display(drivers_df)

# COMMAND ----------

#Read the cell below
#drivers_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

# MAGIC %md
# MAGIC The cell above creates a parquet file in the ADLS, but in the context of notebook "Section18-SQL_Creating_Databases/02-SQL_Create_processed_and_presentation_databases", we're going to change a bit this command to **create a (managed) table** in the Database (mentioned in the notebook) while also writing data to parquet files.<br>
# MAGIC Bear in mind that despite not passing the destination of an ADLS location, this information is **available in the path when the Database was created.**
# MAGIC Being said, the new command will be presented in the cell below.

# COMMAND ----------

drivers_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl102023/processed/drivers"))

# COMMAND ----------

dbutils.notebook.exit("Success")
