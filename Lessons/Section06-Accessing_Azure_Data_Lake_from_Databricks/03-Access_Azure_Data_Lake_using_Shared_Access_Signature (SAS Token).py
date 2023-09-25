# Databricks notebook source
# MAGIC %md
# MAGIC ## 03 - Access_Azure_Data_Lake_using_Shared_Access_Signature 

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Provides fine grained access to the storage: for example, you can allow access to only **Blob containers**, restricting access to files qeues, tables etc.
# MAGIC 2. Allow specific permissions (write / read).
# MAGIC 3. Restrict access to specific time period.
# MAGIC 4. Limit access to specific IP addresses

# COMMAND ----------

# MAGIC %md
# MAGIC In order to access ADLS
# MAGIC
# MAGIC AZURE DATABRICKS -> **SHARED ACCESS SIGNATURE (SAS)** -> AZURE DATA LAKE GEN 2 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### The setting below is specific for **SHARED ACCESS SIGNATURE (SAS)**

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl092023.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl092023.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl092023.dfs.core.windows.net", "SAS_TOKEN")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl092023.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl092023.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


