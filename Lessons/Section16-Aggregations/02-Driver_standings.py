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

driver_standings_df = races_results_df                                          \
                                    .groupBy("race_year", "driver_name", "nationality", "team_name") \
                                    .agg(_sum("points").alias("total_points"),  \
                                         count(when(col("position") == 1, True)).alias("wins"))

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
