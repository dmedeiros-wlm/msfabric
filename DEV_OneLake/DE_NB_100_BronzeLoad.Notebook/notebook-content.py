# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     },
# META     "environment": {}
# META   }
# META }

# CELL ********************

# MAGIC %%configure -f
# MAGIC { 
# MAGIC             "defaultLakehouse": {
# MAGIC                 "name": 'DE_LH_100_BRONZE_WoodlandMills',
# MAGIC                 "id": '20b75530-e6f0-4540-b8a1-5d0af00882ff',
# MAGIC                 "workspaceId": 'ffc83076-e681-4bb9-abb3-1d1e60ca2afd'
# MAGIC             }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import datetime

# Capture the start time
start_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql import functions as F
import json
import logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# ingestsourceschema = "dataverse_woodlandmill_cds2_workspace_unqf1dbc07947a74485af950d8945cb3"
# ingestsourcetable = "account"
# ingestsourcedatecolumn = "modifiedon"
# ingeststartdate = "2024-07-07T02:12:07Z"
# ingestenddate = ""
# sinktablename = "AccountTest"
# loadtype = "incremental"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Start operation by constructing base_path for read and write operations
base_path = "abfss://WP_DEV_OneLake@onelake.dfs.fabric.microsoft.com/"
logger.info(f"Copy operation started for table {ingestsourcetable}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set the table path for the source data
source_path = base_path + f"{ingestsourceschema}.Lakehouse/Tables/{ingestsourcetable}"

# Read data from source path
df = spark.read.format("delta").load(source_path)

logger.info(f"Data read from {source_path}")

# Get stats for how many rows are read at source
rows_read = df.count()

# Define final data by filtering date column by start date and end date if it exists
final_df = df.filter((F.col(ingestsourcedatecolumn) > F.lit(ingeststartdate)) & (F.col(ingestsourcedatecolumn) < F.lit(ingestenddate)) if ingestenddate \
    else (F.col(ingestsourcedatecolumn) > F.lit(ingeststartdate)))

logger.info(f"Data filtered based on {ingestsourcedatecolumn} column")

# Get stats for how many rows will be copied to target
rows_copied = final_df.count()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if loadtype == "incremental":
    # Set the file path for the target data
    target_path = base_path + f"DE_LH_100_BRONZE_WoodlandMills.Lakehouse/Files/incremental/{sinktablename}"

    # Overwrite new data as a parquet file
    final_df.coalesce(1).write.mode("overwrite").format("parquet").save(target_path)

    logger.info(f"Incremental load data saved as parquet at {target_path}")

else:
    # Set the file path for the target data
    target_path = base_path + f"DE_LH_100_BRONZE_WoodlandMills.Lakehouse/Tables/{sinktablename}"

    # Overwrite new data as a delta table
    final_df.coalesce(1).write.mode("overwrite").format("delta").save(target_path)

    logger.info(f"Full load data saved as delta table at {target_path}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

end_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')

# Save stats in a dictionary to be return at notebook's exit
result = {
    "rowsread": rows_read,
    "rowscopied": rows_copied,
    "pipelinestarttime": start_time,
    "pipelineendtime": end_time
}

if loadtype == "full":
    result["deltalkeupdated"] = 0

logger.info(result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Exit the notebook with the result as a json
notebookutils.notebook.exit(json.dumps(result))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
