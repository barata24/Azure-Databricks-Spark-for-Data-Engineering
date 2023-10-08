# Databricks notebook source
# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

#SQL way
races_filtered = races_df.filter("race_year = 2019")
# races_filtered = races_df.filter("race_year = 2019 and round <= 5") # only returns 5 records

# COMMAND ----------

display(races_filtered)

# COMMAND ----------

#Python way
from pyspark.sql.functions import col
races_filtered = races_df.filter(races_df.race_year == 2019)
# races_filtered = races_df.filter(races_df["race_year"] == 2019)
# races_filtered = races_df.filter(col("race_year") == 2019)

#races_filtered = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <= 5)) # same as above "in sql"

# COMMAND ----------

races_filtered.count()

# COMMAND ----------


