# Databricks notebook source
# MAGIC %md
# MAGIC ##11-Mount_ADLS_Containers_for_project

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #Getting secrets from Azure Key Vault
    client_id     = dbutils.secrets.get(scope = "formula1-scope", key = "formula1dl092023-client-id")
    tenant_id     = dbutils.secrets.get(scope = "formula1-scope", key = "formula1dl092023-tenant-id")
    client_secret = dbutils.secrets.get(scope = "formula1-scope", key = "formula1dl092023-client-secret")

    #Set spark configs
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #Mount the storage account container
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    dbutils.fs.mount(
    source        = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net",
    mount_point   = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls("formula1dl092023", "raw")
mount_adls("formula1dl092023", "processed")
mount_adls("formula1dl092023", "presentation")

# COMMAND ----------


