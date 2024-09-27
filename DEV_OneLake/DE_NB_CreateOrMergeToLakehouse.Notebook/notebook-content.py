# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
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
from pyspark.sql import Window
from pyspark.sql import SparkSession
import logging

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Initialize Spark session
spark = SparkSession.builder.appName("Delta Upsert").getOrCreate()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upsert_to_delta(lakehousePath, tableName, tableKey, dateColumn, layer, tableKey2=None):
    logger.info(f"Upsert operation started for table {tableName}")

    # Define partition keys (tableKey and optionally tableKey2)
    partition_keys = [tableKey]
    if tableKey2:
        partition_keys.append(tableKey2)

    deltaTablePath = f"{lakehousePath}/Tables/{tableName}"
    parquetFilePath = f"{lakehousePath}/Files/incremental/{tableName}/{tableName}.parquet" if layer == "gold" else f"{lakehousePath}/Files/incremental/{tableName}"

    # Read and cache the DataFrame from the parquet file
    df = spark.read.parquet(parquetFilePath).cache()

    # Define a window specification to partition by tableKey and order by dateColumn descending
    window_spec = Window.partitionBy(*partition_keys).orderBy(col(dateColumn).desc())

    # Filter and select only the necessary columns
    df2 = df.withColumn("row_number", row_number().over(window_spec)).filter(col("row_number") == 1).drop("row_number")

    if tableKey2 is None:
        mergeKeyExpr = f"t.{tableKey} = s.{tableKey}"
    else:
        mergeKeyExpr = f"t.{tableKey} = s.{tableKey} AND t.{tableKey2} = s.{tableKey2}"

    # Perform checks and set conditions
    delta_table_exists = DeltaTable.isDeltaTable(spark, deltaTablePath)

    if delta_table_exists:
        deltaTable = DeltaTable.forPath(spark, deltaTablePath)
        current_columns = set(deltaTable.toDF().columns)
        new_columns = set(df2.columns)
        is_schema_reset_needed = new_columns.issubset(current_columns) and current_columns.difference(new_columns)

    is_primary_key_present = tableKey

    if (delta_table_exists) and (not is_schema_reset_needed) and (is_primary_key_present):
        # Check if schema evolution is needed
        is_schema_evolution_needed = not new_columns.issubset(current_columns)

        if is_schema_evolution_needed:
            # Append the new schema to the existing Delta table
            df2.limit(0).write.format("delta").mode("append").option("mergeSchema", True).save(deltaTablePath)

            # Reload the existing Delta table with the new schema
            deltaTable = DeltaTable.forPath(spark, deltaTablePath)

        # Update existing data and insert new data
        deltaTable.alias("t").merge(
            df2.alias("s"),
            mergeKeyExpr
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

        # Collect operation metrics
        history = deltaTable.history(1).select("operationMetrics")
        operationMetrics = history.collect()[0]["operationMetrics"]
        numInserted = operationMetrics.get("numTargetRowsInserted", 0)
        numUpdated = operationMetrics.get("numTargetRowsUpdated", 0)
    else:
        # Create Delta table if it doesn't exist or overwrite (including schema) if it exists
        df2.write.format("delta").mode("overwrite").option("overwriteSchema", True).save(deltaTablePath)

        # Collect operation metrics
        deltaTable = DeltaTable.forPath(spark, deltaTablePath)
        history = deltaTable.history(1).select("operationMetrics")
        operationMetrics = history.collect()[0]["operationMetrics"]
        numInserted = operationMetrics.get("numOutputRows", 0)
        numUpdated = 0

    df3 = spark.read.format("delta").load(deltaTablePath)
    maxdate = df3.agg(max(dateColumn)).collect()[0][0]
    maxdate_str = maxdate.strftime("%Y-%m-%d %H:%M:%S")

    logger.info(f"Max date: {maxdate_str}, Rows Inserted: {numInserted}, Rows Updated: {numUpdated}")

    # Initialize the common part of the dictionary
    result = {
        "deltalakeinserted": str(numInserted),
        "deltalakeupdated": str(numUpdated)
    }

    # Add the specific keys based on the layer
    if layer == 'bronze':
        result["ingestmaxdatetime"] = maxdate_str
        result["ingeststartdate"] = maxdate_str
    elif layer == 'gold':
        result["sinkmaxdatetime"] = maxdate_str
        result["sourcestartdate"] = maxdate_str

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# incrload = "[]"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Dictionary of parameters
params_list = json.loads(incrload.replace('\\\"', '"').replace('\"', '"').replace('\"{', '{').replace('}\"', '}'))

# Iterate through the list of parameters and perform the upsert
results = []
for params in params_list:
    core_result = upsert_to_delta(
        params.get("lakehousePath"),
        params.get("tableName"),
        params.get("tableKey"),
        params.get("dateColumn"),
        params.get("layer"),
        params.get("tableKey2") or None
    )
    
    # Initialize the common part of the result
    tablestats = {
        **core_result,
        "loadtype": params.get("loadtype"),
        "batchloaddatetime": params.get("batchloaddatetime"),
        "loadstatus": params["copyStats"].get("loadstatus"),
        "rowsread": params["copyStats"].get("rowsread"),
        "rowscopied": params["copyStats"].get("rowscopied"),
        "pipelinestarttime": params["copyStats"].get("pipelinestarttime"),
        "pipelineendtime": params["copyStats"].get("pipelineendtime")
    }

    # Add specific keys based on the layer
    if params.get("layer") == 'bronze':
        tablestats["ingestsourceschema"] = params.get("ingestsourceschema")
        tablestats["ingestsourcetable"] = params.get("ingestsourcetable")
    elif params.get("layer") == 'gold':
        tablestats["sourceschema"] = params.get("sourceschema")
        tablestats["sourcetable"] = params.get("sourcetable")

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
