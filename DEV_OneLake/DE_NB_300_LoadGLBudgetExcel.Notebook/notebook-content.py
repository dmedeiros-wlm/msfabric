# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "69829626-ddd7-4953-b640-41ce651c827c",
# META       "default_lakehouse_name": "DE_LH_300_GOLD_Finance",
# META       "default_lakehouse_workspace_id": "5ead1c24-51e7-46e4-acc8-39c084284e86"
# META     }
# META   }
# META }

# CELL ********************

import pandas as pd

# Parameters
source_workspaceId = '5ead1c24-51e7-46e4-acc8-39c084284e86'
source_lakehouseId = '69829626-ddd7-4953-b640-41ce651c827c'
target_workspaceId = 'ffc83076-e681-4bb9-abb3-1d1e60ca2afd'
target_lakehouseId = '20b75530-e6f0-4540-b8a1-5d0af00882ff'
tableName = 'glaccountsapprovers'

import pandas as pd

# Define the schema
schema = {
    'glaccountnumber': 'str',
    'glownerapprover': 'str',
    'department': 'str',
}

# Read the Excel file with enforced schema
pandas_df = pd.read_excel(
    f"abfss://{source_workspaceId}@onelake.dfs.fabric.microsoft.com/{source_lakehouseId}/Files/{tableName}.xlsx",
    dtype=schema
)

# Display the data
# display(pandas_df)

# Convert pandas DataFrame into a Spark DataFrame
spark_df = spark.createDataFrame(pandas_df)

# Overwrite new data as a delta table
spark_df.coalesce(1).write.mode("overwrite").format("delta").save(f"abfss://{target_workspaceId}@onelake.dfs.fabric.microsoft.com/{target_lakehouseId}/Tables/{tableName}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
