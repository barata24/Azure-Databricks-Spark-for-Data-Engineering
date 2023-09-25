# Databricks notebook source
# MAGIC %md
# MAGIC ##06-Explore_dbutils_secrets_utility

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="formula1-scope")

# COMMAND ----------

dbutils.secrets.get(scope = "formula1-scope", key = "formula1dl092023-account-key")

# COMMAND ----------

# MAGIC %md
# MAGIC The ouput above makes sure secrets aren't exposed into the notebook.
