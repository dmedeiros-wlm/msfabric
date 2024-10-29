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
# MAGIC                 "name": 'DE_LH_300_GOLD_WoodlandMills',
# MAGIC                 "id": 'e83da213-3bfb-4530-ad8c-9b9b04aa70dd',
# MAGIC                 "workspaceId": 'ffc83076-e681-4bb9-abb3-1d1e60ca2afd'
# MAGIC             }
# MAGIC }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import com.microsoft.spark.fabric
from com.microsoft.spark.fabric.Constants import Constants
from pyspark.sql import functions as F
import json
import logging
import datetime

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get the current date
current_date = datetime.datetime.now()

# Format the date components
year = current_date.strftime("%Y")  # yyyy
month = current_date.strftime("%m")  # MM
day = current_date.strftime("%d")    # dd

# Capture the start time
start_time = current_date.strftime('%Y-%m-%d %H:%M:%S.%f')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# sourceschema = "wlm_tech"
# sourcetable = "tickets"
# sourcedatecolumn = "LastUpdated"
# sourcestartdate = "2024-10-22T22:48:36Z"
# sourceenddate = ""
# sinktable = "ticketsGoldTest"
# loadtype = "incremental"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Start operation by constructing base_path for read and write operations
sourcewarehouse = "DW_WH_200_SILVER_WoodlandMills"
base_path = "abfss://WP_DEV_OneLake@onelake.dfs.fabric.microsoft.com/"
logger.info(f"Copy operation started for table {sourcetable}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Set the warehouse path for the source data
source = f"{sourcewarehouse}.{sourceschema}.{sourcetable}"

# Read data from source
df = spark.read.synapsesql(source)

logger.info(f"Data read from {source}")

# Get stats for how many rows are read at source
rows_read = df.count()

# Define final data by filtering date column by start date and end date if it exists
final_df = df.filter(
    ((F.col(sourcedatecolumn) > F.lit(sourcestartdate)) if sourcestartdate else F.lit(True)) &  # Skip filtering if sourcestartdate is null
    ((F.col(sourcedatecolumn) < F.lit(sourceenddate)) if sourceenddate else F.lit(True))        # Skip filtering if sourceenddate is null
)

logger.info(f"Data filtered based on {sourcedatecolumn} column")

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
    target_path = base_path + f"DE_LH_300_GOLD_WoodlandMills.Lakehouse/Files/Fabric/{sourcewarehouse}/{sourceschema}/{sinktable}/{year}/{year}-{month}/{year}-{month}-{day}/"

    # Overwrite new data as a parquet file
    final_df.coalesce(1).write.mode("overwrite").format("parquet").save(target_path)

    logger.info(f"Incremental load data saved as parquet at {target_path}")

else:
    # Set the file path for the target data
    target_path = base_path + f"DE_LH_300_GOLD_WoodlandMills.Lakehouse/Tables/{sinktable}"

    # Overwrite new data as a delta table
    final_df.coalesce(1).write.mode("overwrite").option("mergeSchema", "true").format("delta").save(target_path)

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
