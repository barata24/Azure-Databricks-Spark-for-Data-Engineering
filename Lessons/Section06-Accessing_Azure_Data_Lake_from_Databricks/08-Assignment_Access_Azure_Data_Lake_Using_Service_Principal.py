# Databricks notebook source
# MAGIC %md
# MAGIC ##08-Assignment_Access_Azure_Data_Lake_Using_Service_Principal

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Register Azure Service Principal
# MAGIC 2. Generate a secret for the Service Principal
# MAGIC 3. Set Spark configs with App/Client ID, Directory/Tenant ID & Secret
# MAGIC 4. Assign the required Role on the Data Lake for the Service Principal so that it gets access to the Data Lake - **Done accessing ADLS IAM properties and add role assignmente as "Storage blob data contributor"**

# COMMAND ----------

dbutils.secrets.list("formula1dl-scope")

# COMMAND ----------

client_id     = dbutils.secrets.get(scope = "formula1-scope", key = "formula1app-client-id")
tenant_id     = dbutils.secrets.get(scope = "formula1-scope", key = "formula1app-tenant-id")
client_secret = dbutils.secrets.get(scope = "formula1-scope", key = "formula1app-client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl092023.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl092023.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl092023.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl092023.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl092023.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl092023.dfs.core.windows.net/circuits.csv"))
