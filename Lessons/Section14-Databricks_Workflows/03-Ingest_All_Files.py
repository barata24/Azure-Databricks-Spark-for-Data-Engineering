# Databricks notebook source
#dbutils.notebook.run(<Notebook_To_Run>, <Period_To_Timeout>, <DICT-{"Widget_var_name", "Value_To_pass"}>)

v_result = dbutils.notebook.run("../Section11-Data_Ingestion_csv/01-Ingesting_Circuits_Data", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("../Section11-Data_Ingestion_csv/02-Ingesting_Races_Data", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("../Section12-Data_Ingestion_json/01-Ingesting_Constructors_Data", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("../Section12-Data_Ingestion_json/02-Ingesting_Drivers_Data", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("../Section12-Data_Ingestion_json/03-Ingesting_Results_Data", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("../Section12-Data_Ingestion_json/04-Ingesting_Pitstops_Data", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("../Section13-Data_Ingestion_Multiple_Files/01-Ingesting_Laptimes_Data", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result

# COMMAND ----------

v_result = dbutils.notebook.run("../Section13-Data_Ingestion_Multiple_Files/02-Ingesting_Qualifying_Data", 0, {"p_data_source": "Eargast API"})

# COMMAND ----------

v_result
