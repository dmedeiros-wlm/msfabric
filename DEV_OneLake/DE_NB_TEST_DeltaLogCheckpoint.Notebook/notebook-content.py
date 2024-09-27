# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "e83da213-3bfb-4530-ad8c-9b9b04aa70dd",
# META       "default_lakehouse_name": "DE_LH_300_GOLD_WoodlandMills",
# META       "default_lakehouse_workspace_id": "ffc83076-e681-4bb9-abb3-1d1e60ca2afd"
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%spark
# MAGIC 
# MAGIC import org.apache.spark.sql.delta.DeltaLog

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%spark
# MAGIC 
# MAGIC DeltaLog.forTable(spark,"Tables/dim_sales_state").checkpoint()

# METADATA ********************

# META {
# META   "language": "scala",
# META   "language_group": "synapse_pyspark"
# META }
