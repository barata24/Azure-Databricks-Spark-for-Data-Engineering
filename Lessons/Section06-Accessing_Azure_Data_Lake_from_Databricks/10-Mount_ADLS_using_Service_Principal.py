# Databricks notebook source
# MAGIC %md
# MAGIC ## 10-Mount_ADLS_using_Service_Principal

# COMMAND ----------

# MAGIC %md
# MAGIC **Service Principal** are quite similar to user accounts like ours.
# MAGIC
# MAGIC They can be registered and assigned permissions required to access the resources in the Azure subscription via **RBAC** (Role Based Access Control)
# MAGIC
# MAGIC **Service Principal** is the recommended method to be used in Databricks because it provides good security.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. **Get Client ID, Tenant ID & Secret from Azure Key Vault**
# MAGIC 2. Set Spark configs with **App/Client ID, Directory/Tenant ID & Secret**
# MAGIC 3. Call file system **utility mount to mount the storage**
# MAGIC 4. Explore other file systems **utilities related to mount (list all mounts, unmount)** 

# COMMAND ----------

dbutils.secrets.list(scope = "formula1dl-scope")

# COMMAND ----------

client_id     = dbutils.secrets.get(scope = "formula1dl-scope", key = "formula1app-client-id")
tenant_id     = dbutils.secrets.get(scope = "formula1dl-scope", key = "formula1app-tenant-id")
client_secret = dbutils.secrets.get(scope = "formula1dl-scope", key = "formula1app-client-secret")

# COMMAND ----------

#With the cell below this code is no longer needed.
""" 
spark.conf.set("fs.azure.account.auth.type.formula1dl092023.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formula1dl092023.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formula1dl092023.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formula1dl092023.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formula1dl092023.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")
"""

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
    source        = "abfss://demo@formula1dl102023.dfs.core.windows.net",
    mount_point   = "/mnt/formula1dl102023/demo",
    extra_configs = configs
)

# COMMAND ----------

# Since we've done a mounting point for this container, we no longer need to specify the actual path below.
# Instead we can use directly the mounting point

#display(dbutils.fs.ls("abfss:/demo@formula1dl102023.dfs.core.windows.net"))
display(dbutils.fs.ls("/mnt/formula1dl102023/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1dl102023/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %md
# MAGIC Let's try to remove a mount with unmount command

# COMMAND ----------

dbutils.fs.unmount("/mnt/formula1dl102023/demo")

# COMMAND ----------


