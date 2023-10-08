# Databricks notebook source
# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Assignment

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

races_df   = spark.read.parquet(f"{processed_folder_path}/races")
results_df = spark.read.parquet(f"{processed_folder_path}/results")
driver_df  = spark.read.parquet(f"{processed_folder_path}/drivers")
constructors_df = spark.read.parquet(f"{processed_folder_path}/constructors")
circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

abu_dhabi_race = races_df.select(races_df.race_id, races_df.name.alias("race_name"), races_df.race_year, races_df.date, races_df.circuit_id)

# COMMAND ----------

display(abu_dhabi_race)

# COMMAND ----------

abu_dhabi_race_result = abu_dhabi_race.join(results_df, abu_dhabi_race.race_id == results_df.race_id, "inner") \
                                      .select(abu_dhabi_race.race_id, abu_dhabi_race.race_name, abu_dhabi_race.race_year, abu_dhabi_race.date, abu_dhabi_race.circuit_id, \
                                              results_df.result_id, results_df.driver_id, results_df.constructor_id, results_df.number,                                   \
                                              results_df.grid, results_df.points, results_df.time, results_df.fastest_lap_time                                            \
                                              )
display(abu_dhabi_race_result)

# COMMAND ----------

abu_dhabi_race_result_drivers = abu_dhabi_race_result.join(driver_df, abu_dhabi_race_result.driver_id == driver_df.driver_id, "inner") \
                                                     .select(abu_dhabi_race_result["*"], driver_df.name.alias("driver_name"), driver_df.nationality)
display(abu_dhabi_race_result_drivers)

# COMMAND ----------

abu_dhabi_race_result_drivers_constructor = abu_dhabi_race_result_drivers.join(constructors_df, abu_dhabi_race_result_drivers.constructor_id == constructors_df.constructorId, "inner") \
                                                                         .select(abu_dhabi_race_result_drivers["*"], constructors_df.name.alias("team_name"))
display(abu_dhabi_race_result_drivers_constructor)

# COMMAND ----------

final_df = abu_dhabi_race_result_drivers_constructor.join(circuits_df, abu_dhabi_race_result_drivers_constructor.circuit_id == circuits_df.circuit_id, "inner") \
                                                    .select(abu_dhabi_race_result_drivers_constructor["*"], circuits_df.name.alias("circuit_name"))
final_df = final_df.select(
    final_df.race_year, final_df.race_name, final_df.circuit_name, final_df.date, final_df.nationality, final_df.driver_name, final_df.number, final_df.team_name, final_df.grid, final_df.fastest_lap_time, final_df.time, final_df.points)
display(final_df)

# COMMAND ----------

final_df = final_df.withColumn("created_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ###Results

# COMMAND ----------

display(final_df.filter((final_df.race_year == 2020) & (final_df.race_name == "Abu Dhabi Grand Prix")))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Write Data to presentation layer (container)

# COMMAND ----------

final_df.write.mode("overwrite").parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------


