# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/02-Common_Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##01-Ingesting_Laptimes_Data

# COMMAND ----------

# MAGIC %md
# MAGIC Note: This data that is about to be ingested is a set of CSV files with the same schema and which are going to be processed simultaneously.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit

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
                   .csv(f"{raw_folder_path}/lap_times/lap_times_split*.csv")

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

laptimes_df = add_ingestion_date(laptimes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding new column from widget

# COMMAND ----------

laptimes_df = laptimes_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

display(laptimes_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing data to Datalake

# COMMAND ----------

#Read the cell below
#laptimes_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

# COMMAND ----------

# MAGIC %md
# MAGIC The cell above creates a parquet file in the ADLS, but in the context of notebook "Section18-SQL_Creating_Databases/02-SQL_Create_processed_and_presentation_databases", we're going to change a bit this command to **create a (managed) table** in the Database (mentioned in the notebook) while also writing data to parquet files.<br>
# MAGIC Bear in mind that despite not passing the destination of an ADLS location, this information is **available in the path when the Database was created.**
# MAGIC Being said, the new command will be presented in the cell below.

# COMMAND ----------

laptimes_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl102023/processed/lap_times"))

# COMMAND ----------

dbutils.notebook.exit("Success")
