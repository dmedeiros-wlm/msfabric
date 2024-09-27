# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   }
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta.tables import *

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

# startyear = 2023
# endyear = 2024
# lakehousePath = "abfss://WP_DEV_OneLake@onelake.dfs.fabric.microsoft.com/Bronze.Lakehouse"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

deltaTablePath = f"{lakehousePath}/Tables/Calendar"
print(deltaTablePath) 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark = SparkSession.builder.appName("Calendar").getOrCreate()

# Set the time parser policy to LEGACY
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Create a DataFrame with a range of dates
dates = spark.range(
    (datetime.now() - datetime(startyear, 1, 1)).days + 1
).select(
    (date_add(lit(f"{startyear}-01-01"), col("id").cast("int"))).alias("date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Select the desired columns
calendardf = dates\
.select(
    "date",
    dayofmonth("date").alias("daynum"),
    dayofweek("date").alias("dayofweeknum"),
    # date_format("date", "e").alias("dayofweeknum"),
    date_format("date", "EEEE").alias("dayofweekname"),
    month("date").alias("monthnum"),
    date_format("date", "MMMM").alias("monthname"),
    date_format("date", "MMM-YY").alias("monthyear"),
    quarter("date").alias("quarternum"),
    concat(lit("Q"), quarter("date")).alias("quartername"),
    year("date").alias("year")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Show the resulting DataFrame
# display(calendardf)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

calendardf.write.format("delta").mode("overwrite").save(deltaTablePath)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
