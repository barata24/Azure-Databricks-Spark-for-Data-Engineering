# Databricks notebook source
# MAGIC %md
# MAGIC ###02-Access_Azure_Data_Lake using_Access_Keys

# COMMAND ----------

# MAGIC %md
# MAGIC While using an **Access Key directly in your notebook**, you have to set it in the **spark configs**.

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC Instead of using HTTP protocol to communicate with the Azure Data Lake, it is **recommended** to use **abfs (Azure Blob File System)**.
# MAGIC It is optimized for big data analytics and offers better security.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### The setting below is specific for Access Key

# COMMAND ----------

##  1 - Set config to access adls
spark.conf.set(
    #"fs.azure.account.key.<storageAccountName>.dfs.core.windows.net",
    #"<SAS_Key>"
)

# COMMAND ----------

## 2 - Create actual communication to list data in adls
display(dbutils.fs.ls("abfss://demo@formula1dl092023.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl092023.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC -------------------------------------
# MAGIC -------------------------------------

# COMMAND ----------

# MAGIC %md
# MAGIC Now that we've created our secret scope **(databricks.net#secrets/createScope)** to access our secrets in the **Azure KeyVault**, let's make use of it.
# MAGIC
# MAGIC **Reminder: This access to the adls intends to show how it should be done using Spark configuration access key pattern**  

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope="formula1-scope")

# COMMAND ----------

formuladl_account_key = dbutils.secrets.get(scope="formula1-scope", key="formula1dl092023-account-key")

# COMMAND ----------

# MAGIC %md
# MAGIC With the same spark config shown above, lets use this newly created key

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formula1dl092023.dfs.core.windows.net",
    formuladl_account_key
)

# COMMAND ----------

display(spark.read.csv("abfss://demo@formula1dl092023.dfs.core.windows.net/circuits.csv"))
