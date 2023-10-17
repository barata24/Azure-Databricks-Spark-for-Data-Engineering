# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/02-Common_Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##03-Ingesting_Results_Data

# COMMAND ----------

from pyspark.sql.types     import StructType, StructField, IntegerType, FloatType, StringType, DataType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ####Creating schema 

# COMMAND ----------

results_schema = StructType(fields=[
    StructField("resultId",        IntegerType(), False), \
    StructField("raceId",          IntegerType(), True), \
    StructField("driverId",        IntegerType(), True), \
    StructField("constructorId",   IntegerType(), True), \
    StructField("number",          IntegerType(), True), \
    StructField("grid",            IntegerType(), True), \
    StructField("position",        IntegerType(), True), \
    StructField("positionText",    StringType(),  True), \
    StructField("positionOrder",   StringType(),  True), \
    StructField("points",          FloatType(),   True), \
    StructField("laps",            IntegerType(), True), \
    StructField("time",            StringType(),  True), \
    StructField("milliseconds",    IntegerType(), True), \
    StructField("fastestLap",      IntegerType(), True), \
    StructField("rank",            IntegerType(), True), \
    StructField("fastestLapTime",  StringType(),  True), \
    StructField("fastestLapSpeed", FloatType(),   True), \
    StructField("statusId",        StringType(),  True)  \
])

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Reading data

# COMMAND ----------

results_df = spark.read \
                  .schema(results_schema) \
                  .json(f"{raw_folder_path}/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming cols

# COMMAND ----------

results_df = results_df.withColumnRenamed("resultId",        "result_id") \
                       .withColumnRenamed("raceId",          "race_id") \
                       .withColumnRenamed("driverId",        "driver_id") \
                       .withColumnRenamed("constructorId",   "constructor_id") \
                       .withColumnRenamed("positionText",    "position_text") \
                       .withColumnRenamed("positionOrder",   "position_order") \
                       .withColumnRenamed("fastestLap",      "fastest_lap") \
                       .withColumnRenamed("fastestLapTime",  "fastest_lap_time") \
                       .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding col

# COMMAND ----------

results_df = add_ingestion_date(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding new column from widget

# COMMAND ----------

results_df = results_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Drop Col

# COMMAND ----------

results_df = results_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Write to Datalake

# COMMAND ----------

#Read the cell below
#results_df.write.mode("overwrite").partitionBy("race_id").parquet(f"{processed_folder_path}/results")

# COMMAND ----------

# MAGIC %md
# MAGIC The cell above creates a parquet file in the ADLS, but in the context of notebook "Section18-SQL_Creating_Databases/02-SQL_Create_processed_and_presentation_databases", we're going to change a bit this command to **create a (managed) table** in the Database (mentioned in the notebook) while also writing data to parquet files.<br>
# MAGIC Bear in mind that despite not passing the destination of an ADLS location, this information is **available in the path when the Database was created.**
# MAGIC Being said, the new command will be presented in the cell below.

# COMMAND ----------

results_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl102023/processed/results"))

# COMMAND ----------

dbutils.notebook.exit("Success")
