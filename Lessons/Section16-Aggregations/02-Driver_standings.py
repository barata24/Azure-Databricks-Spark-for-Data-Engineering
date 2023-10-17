# Databricks notebook source
# MAGIC %md
# MAGIC ####Produce driver standings

# COMMAND ----------

from pyspark.sql.functions import sum as _sum, count, when, col, desc

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

races_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

driver_standings_df = races_results_df                                                               \
                                    .groupBy("race_year", "driver_name", "nationality", "team_name") \
                                    .agg(_sum("points").alias("total_points"),                       \
                                         count(when(col("position") == 1, True)).alias("wins")
                                         )

# COMMAND ----------

display(driver_standings_df.filter(col("race_year") == 2020).orderBy(col("total_points").desc()))

# COMMAND ----------

from pyspark.sql.window    import Window
from pyspark.sql.functions import rank

# COMMAND ----------

#partitionBy is race_year beacuse we're ranking the driver within the race year
#orderBy total_points and wins will provide a rank based on this two conditions. If a two drivers have the same total_points and wins they end in the same position, but if one driver has one more win than the other, it'll differentiate them in the rank.
driver_rank_spec = Window.partitionBy("race_year").orderBy(col("total_points").desc(), col("wins").desc())
final_df         = driver_standings_df.withColumn("rank", rank().over(driver_rank_spec))

# COMMAND ----------

display(final_df.filter(col("race_year") == 2020))

# COMMAND ----------

#Read the cell below
#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/driver_standings")

# COMMAND ----------

# MAGIC %md
# MAGIC The cell above creates a parquet file in the ADLS, but in the context of notebook "Section18-SQL_Creating_Databases/02-SQL_Create_processed_and_presentation_databases", we're going to change a bit this command to **create a (managed) table** in the Database (mentioned in the notebook) while also writing data to parquet files.<br>
# MAGIC Bear in mind that despite not passing the destination of an ADLS location, this information is **available in the path when the Database was created.**
# MAGIC Being said, the new command will be presented in the cell below.

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")
