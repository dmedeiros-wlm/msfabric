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
# META       "default_lakehouse_workspace_id": "ffc83076-e681-4bb9-abb3-1d1e60ca2afd"
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create DataFrame
base_df = spark.sql("SELECT activityid, parentactivityid FROM dataverse_woodlandmill_cds2_workspace_unqf1dbc07947a74485af950d8945cb3.email")

# You can use the iterative approach to replicate the recursive CTE logic
previous_count = 0
limit = 1000  # Just as a safety precaution to avoid infinite loops
iteration = 0

while True and iteration < limit:
    # Join base_df with tableOne on the recursive condition
    recursive_join = base_df.alias("recursiveCTE").join(
        spark.table("tableOne").alias("ol2"),
        F.expr("nvl(ol2.reference_line_id, nvl(ol2.source_document_line_id, to_number(ol2.return_attribute2))) = recursiveCTE.columnTwo")
        & (F.col("ol2.columnOne").isin(spark.sql("select header_ids.columnOne from header_ids")))
        & (F.col("ol2.cancelled_flag") == 'N')
        & (F.col("ol2.columnThree") == 1007),
        "inner"
    ).select(
        F.col("ol2.columnOne"),
        F.col("ol2.columnTwo"),
        F.col("ol2.columnThree"),
        F.col("recursiveCTE.columnTwo"),
        (F.col("recursiveCTE.columnFive") + 1).alias("columnFive"),
        F.col("recursiveCTE.columnSix"),
        F.col("ol2.columnSeven"),
        (F.col("ol2.columnEight") * -1).alias("columnEight")
    )
    
    new_count = recursive_join.count()
    
    # Check if we have any new rows
    if new_count == previous_count:
        break
    else:
        base_df = base_df.union(recursive_join)
        previous_count = new_count
        iteration += 1

# base_df now contains the full result
base_df.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
