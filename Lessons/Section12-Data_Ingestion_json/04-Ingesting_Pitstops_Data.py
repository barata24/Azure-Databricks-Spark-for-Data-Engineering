# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/02-Common_Functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ####04-Ingesting_Pitstops_Data

# COMMAND ----------

# MAGIC %md
# MAGIC Note: This JSON file is not as the previous ones since this has multiline JSON objects instead of single line objects 

# COMMAND ----------

from pyspark.sql.types     import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

pitstops_schema = StructType(fields=[ \
    StructField("raceId",      IntegerType(), False), \
    StructField("driverId",    IntegerType(), True), \
    StructField("stop",        StringType(),  True), \
    StructField("lap",         IntegerType(), True), \
    StructField("time",        StringType(),  True), \
    StructField("duration",    StringType(),  True), \
    StructField("milliseconds", IntegerType(), True)  \
])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Read data to Dataframe

# COMMAND ----------

pitstops_df = spark.read \
                   .schema(pitstops_schema) \
                   .option("multiLine", True) \
                   .json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(pitstops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Renaming cols

# COMMAND ----------

pitstops_df = pitstops_df.withColumnRenamed("raceId", "race_id") \
                         .withColumnRenamed("driverId", "driver_id")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding cols

# COMMAND ----------

pitstops_df = add_ingestion_date(pitstops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding new column from widget

# COMMAND ----------

pitstops_df = pitstops_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing data to Datalake

# COMMAND ----------

#Read the cell below
#pitstops_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

# MAGIC %md
# MAGIC The cell above creates a parquet file in the ADLS, but in the context of notebook "Section18-SQL_Creating_Databases/02-SQL_Create_processed_and_presentation_databases", we're going to change a bit this command to **create a (managed) table** in the Database (mentioned in the notebook) while also writing data to parquet files.<br>
# MAGIC Bear in mind that despite not passing the destination of an ADLS location, this information is **available in the path when the Database was created.**
# MAGIC Being said, the new command will be presented in the cell below.

# COMMAND ----------

pitstops_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")
