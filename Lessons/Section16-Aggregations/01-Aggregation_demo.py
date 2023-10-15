# Databricks notebook source
# MAGIC %run "../Section14-Databricks_Workflows/01-Configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Aggregate functions demo

# COMMAND ----------

# MAGIC %md
# MAGIC #####Built-in Aggregate functions

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

demo_df = race_results_df.filter(race_results_df.race_year == 2020)

# COMMAND ----------

display(demo_df)
demo_df.count()

# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, col

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

#Total number of points won on last year's race
demo_df.select(sum("points")).show()

# COMMAND ----------

#Total number of points Hamilton won on last year's race
demo_df.filter("driver_name = 'Lewis Hamilton'").select(sum("points")).show()
demo_df.filter(demo_df.driver_name == "Lewis Hamilton").select(sum("points")).show()

# COMMAND ----------

#Multiple functions in the same select statement - number of points won, number of races he has been into
demo_df.filter(demo_df.driver_name == "Lewis Hamilton").select(sum("points").alias("total_point"),
                                                               countDistinct("race_name").alias("number_of_races")).show()

# COMMAND ----------

display(demo_df \
        .groupBy("driver_name") \
        .sum("points") \
        .orderBy(col("sum(points)").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ###AGG function
# MAGIC To use multiple agg functions you shoud use the **agg function.**<br>
# MAGIC It's not possible to apply a transformation as a sequence of aggregations as below:<br><br>
# MAGIC demo_df \
# MAGIC .groupBy("driver_name") \
# MAGIC .sum("points") \ #(1st agg func) <br>
# MAGIC .countDistinct("race_name") \ #(2nd agg func) -> ERROR <br>
# MAGIC .orderBy(col("sum(points)").desc())

# COMMAND ----------

display(demo_df \
        .groupBy("driver_name") \
        .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")))

# COMMAND ----------

# MAGIC %md
# MAGIC ###Window function

# COMMAND ----------

demo_df = race_results_df.filter("race_year IN (2019, 2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df \
                .groupBy("race_year", "driver_name") \
                .agg(sum("points").alias("total_points"), countDistinct("race_name").alias("number_of_races")) \
                .orderBy(col("race_year").desc(), col("total_points").desc(), col("number_of_races").desc())

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# COMMAND ----------

driverRankSpec = Window.partitionBy("race_year").orderBy(col("total_points").desc())

display(demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)))

# COMMAND ----------



# COMMAND ----------


