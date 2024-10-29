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
# META     }
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

import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# tableParams = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

loadGoldNb = "DE_NB_300_GoldLoad"
incrUpsertNb = "DE_NB_CreateOrMergeToLakehouse"
fullWriteNb = "DE_NB_GetMaxDateFromDeltaTable"
lhGoldPath = "abfss://WP_DEV_OneLake@onelake.dfs.fabric.microsoft.com/DE_LH_300_GOLD_WoodlandMills.Lakehouse"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

params_list = json.loads(tableParams.replace('\\\"', '"').replace('\"', '"').replace('\"{', '{').replace('}\"', '}'))

# Iterate through the list of parameters
tables = []
for params in params_list:    
    # Initialize the common part of the result
    parameters = {
        "sourceschema": params["tableParams"].get("sourceschema"),
        "sourcetable": params["tableParams"].get("sourcetable"),
        "loadtype": params["tableParams"].get("loadtype"),
        "sourcedatecolumn": params["tableParams"].get("sourcedatecolumn"),
        "sourcestartdate": params["tableParams"].get("sourcestartdate"),
        "sourceenddate": params["tableParams"].get("sourceenddate"),
        "sinktable": params["tableParams"].get("sinktable"),
        "tableKey": params["tableParams"].get("tableKey"),
        "tableKey2": params["tableParams"].get("tableKey2"),
        "batchloaddatetime": params["tableParams"].get("batchloaddatetime")
    }

    # Combine the core result with additional information
    table = {
        "tableResults": parameters
    }

    # Append the full result to the tables list
    tables.append(table)

# Initialize the DAG structure
DAG = {
    "activities": [],
    "timeoutInSeconds": 3600,  # max 1 hour for the entire pipeline
    "concurrency": 6  # max 6 notebooks in parallel
}

# Template for each activity
activity_template = {
    "name": "",
    "path": loadGoldNb,
    "args": {},
    "timeoutPerCellInSeconds": 900,
    "retry": 1,
    "retryIntervalInSeconds": 10
}

# Construct activities from the JSON input
for table in tables:
    table_result = table["tableResults"]
    activity = activity_template.copy()
    activity["name"] = f"{loadGoldNb}_{table_result['sinktable']}"
    activity["args"] = table_result
    DAG["activities"].append(activity)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Validate DAG before running
notebookutils.notebook.validateDAG(DAG)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Configuration for DAG graph display
config = {"showTime": True, "displayDAGViaGraphviz":True, "DAGLayout":"planar"}

# Run the notebooks concurrently and capture the results
runResults = notebookutils.notebook.runMultiple(DAG, config)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ExitValue: {"rowsread": 30490, "rowscopied": 113, "pipelinestarttime": "2024-10-24 18:36:22.922905", "pipelineendtime": "2024-10-24 18:40:10.604612"}


# CELL ********************

# Process the results to capture the exit values and statuses
exit_values = {}

for activity_name, result in runResults.items():
    exit_val = json.loads(result['exitVal'])
    exit_val['loadstatus'] = "Succeeded" if result.get('exception') is None else "Failed"
    exit_values[activity_name] = exit_val

# Initialize the lists
incr_list = []
full_list = []

# Iterate through the tables and filter based on loadtype
for loaded_table in tables:
    loaded_results = loaded_table["tableResults"]
    activity_name = f"{loadGoldNb}_{loaded_results['sinktable']}"
    copy_stats = exit_values.get(activity_name, {})
    final_dict = {
        "layer": "gold",
        "loadtype": loaded_results.get("loadtype"),
        "batchloaddatetime": loaded_results.get("batchloaddatetime"),
        "sourceschema": loaded_results.get("sourceschema"),
        "sourcetable": loaded_results.get("sourcetable"),
        "lakehousePath": lhGoldPath,
        "tableName": loaded_results.get("sinktable"),
        "tableKey": loaded_results.get("tableKey"),
        "tableKey2": loaded_results.get("tableKey2"),
        "copyStats": copy_stats
    }
    if loaded_results.get("loadtype") == "incremental":
        final_dict["dateColumn"] = "LastUpdated"
        incr_list.append(final_dict)
    elif loaded_results.get("loadtype") == "full":
        full_list.append(final_dict)

# Convert the lists to JSON strings
incr_list_json = json.dumps(incr_list)
full_list_json = json.dumps(full_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pass the JSON string to the notebook for Incremental Loads and Runs Stats
upsert_result = notebookutils.notebook.run(incrUpsertNb, 600, {"incrload": incr_list_json})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Pass the JSON string to the notebook for Full Runs Stats
write_result = notebookutils.notebook.run(fullWriteNb, 600, {"fullload": full_list_json})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Function to determine the final result based on the given conditions
def get_final_result(upsert_result, write_result):
    if len(upsert_result) == 2:
        return write_result
    elif len(write_result) == 2:
        return upsert_result
    else:
        combined_result = upsert_result[:-1] + "," + write_result[1:]
        return combined_result

# Get the final result
final_result = get_final_result(upsert_result, write_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

notebookutils.notebook.exit(final_result)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
