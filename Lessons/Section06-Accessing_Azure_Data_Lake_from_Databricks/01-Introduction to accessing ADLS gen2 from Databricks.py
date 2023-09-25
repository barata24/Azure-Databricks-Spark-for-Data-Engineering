# Databricks notebook source
# MAGIC %md
# MAGIC In order to **access an Azure Data Lake Storage (Gen2)** you have several possibilities:
# MAGIC
# MAGIC 1. **Storage Account Keys**
# MAGIC 2. **Shared Acceess Signature (SAS Token)** - Provides a more granular access than the 1st option
# MAGIC 3. **Service Principal**

# COMMAND ----------

# MAGIC %md
# MAGIC ###There are several types of **Authentication Scopes**:
# MAGIC
# MAGIC ####"Databricks only" provided
# MAGIC
# MAGIC
# MAGIC 1. **Session Scoped Authentication** - Where credentials are putted in the notebook, creating a session where you have permission to access the ADLS until the notebook gets detached from the cluster.
# MAGIC 2. **Cluster Scoped Authentication** - Valid from the point the cluster starts leading to granted access to all notebooks attached to this cluster
# MAGIC
# MAGIC
# MAGIC #### "Azure provided"
# MAGIC
# MAGIC
# MAGIC 3. **Azure Active Directory Pass-Through Authentication** - Where you enable the cluster to use Active Directory Pass-through authentication. So whenever a user runs a notebook, the Cluster will use the user's Azure Active Directory credentials and look for the roles the user has been assigned to the ADLS using IAM (Identity Access Management) 
# MAGIC 4. **Unity Catalog** - The Admins define the access permissions for an user using Databricks. In this case, the cluster will check if the user has the required permissions to access the ADLS.

# COMMAND ----------


