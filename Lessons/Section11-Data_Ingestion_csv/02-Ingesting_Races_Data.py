# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/02-Common_Functions"

# COMMAND ----------

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
                .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

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

races_df = add_ingestion_date(races_df)

# COMMAND ----------

races_df = races_df.withColumn("race_timestamp", to_timestamp(concat(races_df.date, lit(" "), races_df.time), "yyyy-MM-dd HH:mm:ss")) \
                   .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Adding new column from widget

# COMMAND ----------

races_df = races_df.withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Writing output to Datalake

# COMMAND ----------

#Read the cell below
#races_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# MAGIC %md
# MAGIC The cell above creates a parquet file in the ADLS, but in the context of notebook "Section18-SQL_Creating_Databases/02-SQL_Create_processed_and_presentation_databases", we're going to change a bit this command to **create a (managed) table** in the Database (mentioned in the notebook) while also writing data to parquet files.<br>
# MAGIC Bear in mind that despite not passing the destination of an ADLS location, this information is **available in the path when the Database was created.**
# MAGIC Being said, the new command will be presented in the cell below.

# COMMAND ----------

races_df.write.mode("overwrite").format("parquet").partitionBy("race_year").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM f1_processed.races

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1dl102023/processed/"))

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dl102023/processed/races"))

# COMMAND ----------

dbutils.notebook.exit("Success")
