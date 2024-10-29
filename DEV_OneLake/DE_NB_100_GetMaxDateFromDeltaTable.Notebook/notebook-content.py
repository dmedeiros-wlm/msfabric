# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
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

import json
from delta.tables import *
from pyspark.sql.functions import *
from datetime import datetime
import re
import logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# fullload = []

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark session
spark = SparkSession.builder.appName("Delta Full Load").getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def parse_and_format_date(date_str):
    # Regex pattern to match and capture date components
    date_pattern = re.compile(r'(\d{4})[-/](\d{2})[-/](\d{2})[ T](\d{2})[:.](\d{2})[:.](\d{2})')
    isMatch = date_pattern.match(date_str)
    
    if isMatch:
        print(f"Date string matches RegEx! Input: {date_str}")
        year, month, day, hour, minute, second = isMatch.groups()
        # Normalize the date string to the desired format
        normalized_date_str = f"{year}-{month}-{day} {hour}:{minute}:{second}"
        return normalized_date_str
    
    # If no match, return the original date string
    return date_str

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def full_delta_load(lakehousePath, tableName, dateColumn):
    # Construct the Delta Table path with given inputs
    deltaTablePath = f"{lakehousePath}/Tables/{tableName}"

    # Print the Delta Table path
    # print(deltaTablePath)

    # Read the Delta table
    df = spark.read.format("delta").load(deltaTablePath)

    # Get the maximum date (assuming it can be either a string or a date)
    maxdate = df.agg(max(dateColumn)).collect()[0][0]

    # Get the row count
    rowcount = df.count()
    
    # Check if maxdate is a string and convert it if necessary
    if isinstance(maxdate, str):
        # Parse and format the date string
        maxdate_str = parse_and_format_date(maxdate)
    else:
        # If maxdate is not a string, assume it's a date and format it
        maxdate_str = maxdate.strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Max date: {maxdate_str}, Rows Inserted: {rowcount}")

    # Format the result dict
    result = {
        "ingestmaxdatetime": maxdate_str,
        "deltalakeinserted": str(rowcount)
    }

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dictionary of parameters
params_list = json.loads(fullload.replace('\\\"', '"').replace('\"', '"').replace('\"{', '{').replace('}\"', '}'))

# Iterate through the list of parameters and perform the upsert
results = []
for params in params_list:
    if params.get("layer") == "bronze":
        core_result = full_delta_load(
            params.get("lakehousePath"),
            params.get("tableName"),
            params.get("dateColumn")
        )
    else:
        core_result = {
            "deltalakeinserted": params["copyStats"].get("rowscopied")
        }
    
    # Initialize the common part of the result
    tablestats = {
        **core_result,
        "loadtype": params.get("loadtype"),
        "batchloaddatetime": params.get("batchloaddatetime"),
        "ingestsourceschema" if params.get("layer") == "bronze" else "sourceschema": params.get("ingestsourceschema" if params.get("layer") == "bronze" else "sourceschema"),
        "ingestsourcetable" if params.get("layer") == "bronze" else "sourcetable": params.get("ingestsourcetable" if params.get("layer") == "bronze" else "sourcetable"),
        "loadstatus": params["copyStats"].get("loadstatus"),
        "rowsread": params["copyStats"].get("rowsread"),
        "rowscopied": params["copyStats"].get("rowscopied"),
        "deltalakeupdated": params["copyStats"].get("deltalakeupdated"),
        "pipelinestarttime": params["copyStats"].get("pipelinestarttime"),
        "pipelineendtime": params["copyStats"].get("pipelineendtime")
    }
    
    # Combine the core result with additional information
    full_result = {
        "tablestats": tablestats
    }

    # Append the full result to the results list
    results.append(full_result)

# Print or return the results
for result in results:
    logger.info(result)
    print(result)

notebookutils.notebook.exit(json.dumps(results))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
