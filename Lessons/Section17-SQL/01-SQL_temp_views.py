# Databricks notebook source
# MAGIC %md
# MAGIC ##01-SQL_temp_views

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Temp Views
# MAGIC #####Objectives:
# MAGIC 1. Create temp views on Dataframes
# MAGIC 2. Access the view from **SQL cell**
# MAGIC 3. Access the view from **Python cell**

# COMMAND ----------

race_result_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_result_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM v_race_results
# MAGIC WHERE race_year = 2020

# COMMAND ----------

race_results_2019_df = spark.sql("SELECT * FROM v_race_results WHERE race_year = 2019")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ###Global Views
# MAGIC #####Objectives:
# MAGIC 1. Create **global** views on Dataframes
# MAGIC 2. Access the view from **SQL cell**
# MAGIC 3. Access the view from **Python cell**
# MAGIC 4. Access the view from another notebook

# COMMAND ----------

race_result_df.createGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %md
# MAGIC The next command will fail because spark will register this view on a database called **"global_temp"**

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM gv_race_results

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM global_temp.gv_race_results

# COMMAND ----------

display(spark.sql("SELECT * FROM global_temp.gv_race_results"))

# COMMAND ----------


