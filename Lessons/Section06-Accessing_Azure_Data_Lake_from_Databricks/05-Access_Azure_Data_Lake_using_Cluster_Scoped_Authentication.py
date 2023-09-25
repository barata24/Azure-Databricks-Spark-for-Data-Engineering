# Databricks notebook source
# MAGIC %md
# MAGIC ##05-Access_Azure_Data_Lake_using_Cluster_Scoped_Authentication

# COMMAND ----------

# MAGIC %md
# MAGIC Instead of providing a secret configuration to each notebook, the secret will be assign at the cluster level providing further access to every notebook.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Set the spark config fs.azure.account.key in the cluster advanced configs**
# MAGIC 2. **List files from container demo**
# MAGIC 3. **Read data from circuits.csv**

# COMMAND ----------

# MAGIC %md
# MAGIC Regarding the **first step** shown above, the spark config should look like a key value pair space separated, like this:
# MAGIC
# MAGIC
# MAGIC **fs.azure.account.key.formula1dl092023.dfs.core.windows.net** "space" **Storage_Account_Access_Key**

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl092023.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl092023.dfs.core.windows.net"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##06-Access_Azure_Data_Lake_using_Credential_Passthrough

# COMMAND ----------

# MAGIC %md
# MAGIC In the Azure Storage Account you must provide access to the user using "Access Control (IAM)"
# MAGIC 1. Add -> Role Assignment -> Blob Storage Contributor (gives full access)
# MAGIC 2. Select member 

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formula1dl092023.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl092023.dfs.core.windows.net/circuits.csv"))
