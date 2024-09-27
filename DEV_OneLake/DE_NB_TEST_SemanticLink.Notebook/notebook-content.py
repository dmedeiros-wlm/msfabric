# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9e412763-0d07-4c90-960f-2bd28ae46678",
# META       "default_lakehouse_name": "dataverse_woodlandmill_cds2_workspace_unqf1dbc07947a74485af950d8945cb3",
# META       "default_lakehouse_workspace_id": "ffc83076-e681-4bb9-abb3-1d1e60ca2afd",
# META       "known_lakehouses": [
# META         {
# META           "id": "9e412763-0d07-4c90-960f-2bd28ae46678"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%pip install semantic-link-labs

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import sempy_labs as labs
from sempy_labs import migration, report, directlake
from sempy_labs import lakehouse as lake
from sempy_labs.tom import connect_semantic_model

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

workspace='DEV_TechnicalAdvisors'
dataset='tickets'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

labs.vertipaq_analyzer(workspace=workspace, dataset=dataset)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

labs.run_model_bpa(workspace=workspace, dataset=dataset)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
