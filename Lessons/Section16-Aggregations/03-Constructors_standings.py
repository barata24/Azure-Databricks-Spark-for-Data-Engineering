# Databricks notebook source
# MAGIC %md
# MAGIC ## 03-Constructors_standings

# COMMAND ----------

# MAGIC %md
# MAGIC Now we want to aggregate data in order to achieve the number of wins and points that each Team has won with both their drivers

# COMMAND ----------

# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

from pyspark.sql.functions import col, desc, asc, when, count, sum as _sum

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_df = race_results_df.select("race_year", "team_name", "points", "position")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

constructors_standings_df = race_results_df.groupBy("race_year", "team_name")                          \
                                           .agg(_sum(col("points")).alias("total_points"),             \
                                                count(when(col("position") == 1, True)).alias("wins")) \
                                           .orderBy(col("wins").desc()).filter(col("race_year") == 2020)

# COMMAND ----------

from pyspark.sql.window    import Window
from pyspark.sql.functions import rank

# COMMAND ----------

constructor_rank_spec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
final_df              = constructors_standings_df.withColumn("rank", rank().over(constructor_rank_spec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

#Read the cell below
#final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/constructor_standings")

# COMMAND ----------

# MAGIC %md
# MAGIC The cell above creates a parquet file in the ADLS, but in the context of notebook "Section18-SQL_Creating_Databases/02-SQL_Create_processed_and_presentation_databases", we're going to change a bit this command to **create a (managed) table** in the Database (mentioned in the notebook) while also writing data to parquet files.<br>
# MAGIC Bear in mind that despite not passing the destination of an ADLS location, this information is **available in the path when the Database was created.**
# MAGIC Being said, the new command will be presented in the cell below.

# COMMAND ----------

final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.constructor_standings")
