# Databricks notebook source
# MAGIC %md
# MAGIC ## 04-Access_Azure_Data_Lake_using_Service_Principal

# COMMAND ----------

# MAGIC %md
# MAGIC **Service Principal** are quite similar to user accounts like ours.
# MAGIC
# MAGIC They can be registered and assigned permissions required to access the resources in the Azure subscription via **RBAC** (Role Based Access Control)
# MAGIC
# MAGIC **Service Principal** is the recommended method to be used in Databricks because it provides good security.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Register Azure Service Principal**
# MAGIC 2. **Generate a secret for the Service Principal**
# MAGIC 3. **Set Spark configs with App/Client ID, Directory/Tenant ID & Secret**
# MAGIC 4. **Assign the required Role on the Data Lake for the Service Principal so that it gets access to the Data Lake** 

# COMMAND ----------

client_id     = "client_id"
tenant_id     = "tenant_id"
client_secret = "client_secret"

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formula1dl092023.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl092023.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl092023.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl092023.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl092023.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC Now we need to assign a **Contributor Role to this Service Principal**. <br>
# MAGIC To do that, we need to go to the Storage Account - Access Control (IAM)

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl092023.dfs.core.windows.net"))

# COMMAND ----------


