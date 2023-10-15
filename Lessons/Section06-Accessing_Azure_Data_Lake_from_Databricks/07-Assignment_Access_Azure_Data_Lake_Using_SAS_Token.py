# Databricks notebook source
# MAGIC %md
# MAGIC ##07-Assignment_Access_Azure_Data_Lake_Using_SAS_Token

# COMMAND ----------

# MAGIC %md
# MAGIC 0. Generate a SAS Token and create a secret for it
# MAGIC 1. Set the spark configs for SAS Token
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

# Get stored secrets
dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("formula1dl-scope")

# COMMAND ----------

formula1dl_SAS_Token = dbutils.secrets.get(scope = "formula1dl-scope", key = "formula1dl092023-sastoken")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl092023.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formula1dl092023.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formula1dl092023.dfs.core.windows.net", formula1dl_SAS_Token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl092023.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl092023.dfs.core.windows.net/circuits.csv"))
