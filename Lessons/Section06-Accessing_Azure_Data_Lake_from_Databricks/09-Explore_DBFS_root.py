# Databricks notebook source
# MAGIC %md
# MAGIC ##09-Explore_DBFS_root

# COMMAND ----------

# MAGIC %md
# MAGIC Ingest data to DBFS root (DBFS browser). (Note that you may need to allow DBFS browser at User Settings - Advanced settings) 

# COMMAND ----------

display(dbutils.fs.ls("/FileStore/tables"))

# COMMAND ----------

display(spark.read.csv("dbfs:/FileStore/tables/circuits.csv"))

# COMMAND ----------


