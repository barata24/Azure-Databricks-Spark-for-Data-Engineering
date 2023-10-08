# Databricks notebook source
# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Inner join

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")
races_df = races_df.filter(races_df["race_year"] == 2019) \
                   .withColumnRenamed("name", "race_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
circuits_df = circuits_df.withColumnRenamed("name", "circuit_name")

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df,circuits_df.circuit_id == races_df.circuit_id, "inner") \
                              .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# Outer join -> Left Outer join; Right Outer Join; Full Outer Join

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits")
circuits_df = circuits_df.withColumnRenamed("name", "circuit_name") \
                         .filter(circuits_df.circuit_id < 70)

# COMMAND ----------

# Outputs the circuits that don't have races 
race_circuits_df_r = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "right") \
                             .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)
race_circuits_df_l = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "left") \
                                .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Full Outer join

# COMMAND ----------

race_circuits_df_f = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "full") \
                             .select(circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

# COMMAND ----------

display(race_circuits_df_f)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Semi join

# COMMAND ----------

# MAGIC %md
# MAGIC The difference to a Inner join is that the returned columns belong to the "left" Dataframe

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Anti join

# COMMAND ----------

# MAGIC %md
# MAGIC Anti Join returns everything that is on the left Dataframe that is not in the right Dataframe.

# COMMAND ----------

race_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cross join

# COMMAND ----------

# MAGIC %md
# MAGIC Cross Join applies a cartesian product, it takes every record from the left joins with every record on the right and delivers the product of both.

# COMMAND ----------

race_circuits_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_circuits_df)
