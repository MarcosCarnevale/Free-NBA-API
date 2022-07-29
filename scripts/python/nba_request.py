#==============================================================================================================================
# Encoding: UTF-8                                                                                                             #
# Date: 2022-07-29                                                                                                            #
# Version: 1.0                                                                                                                #
# Project: Request data from NBA API                                                                                          #
# Description: Collect and handle api data https://free-nba.p.rapidapi.com/players                                            #
#              and finally, make available in hive table                                                                      # 
# Author: Marcos Carnevale                                                                                                    #
# Contribution:                                                                                                               #
# Reviewer:                                                                                                                   #
# Programmer linguagem: Spark 2.4.5, Python 3.6.9                                                                             #
#==============================================================================================================================
# Import Library pyspark
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.context import *

# Import Library python
import requests
import json
import logging
import sys
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#==============================================================================================================================
# Create Spark Session
sc = (SparkSession.builder.master("local").appName("NBA").
        enableHiveSupport().
    getOrCreate())

sqlc = SQLContext(sc)

# Set hive.exec.dynamic.partition.mode=no
sqlc.sql("set hive.exec.dynamic.partition.mode=no")

#==============================================================================================================================
# Read Credential file
ini = sys.argv[1]

# Read conf File .ini line by line
with open(ini) as f:
    data = f.readlines()

dict_data = {}
for i in data:
    # Split line by =
    k = i.split('=')[0]
    v = i.split('=')[1]
    # Remove \n
    v = v.replace('\n', '')
    k = k.replace('\n', '')
    # Update dict
    dict_data[k] = v

host = dict_data['X-RapidAPI-Host']
key = dict_data['X-RapidAPI-Key']

#==============================================================================================================================
# Define url and querystring to get data from NBA API
url = f"https://{host}/players"
querystring = {"page":"0","per_page":"25"}
headers = {
	"X-RapidAPI-Key": key,
	"X-RapidAPI-Host": host
}

#==============================================================================================================================
# Define function to get data from NBA API
def get_data(url, querystring, **headers):
    response = requests.request("GET", url, headers=headers, params=querystring)
    if response.status_code == 200:
        return json.loads(response.text)
    else:
        logger.error("Error: %s" % response.status_code)
        return None


#==============================================================================================================================
# Create StructType to define schema
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("height_feet", IntegerType(), True),
    StructField("height_inches", IntegerType(), True),
    StructField("last_name", StringType(), True),
    StructField("position", StringType(), True),
    StructField("team", StructType([
        StructField("id", IntegerType(), True),
        StructField("abbreviation", StringType(), True),
        StructField("city", StringType(), True),
        StructField("conference", StringType(), True),
        StructField("division", StringType(), True),
        StructField("full_name", StringType(), True),
        StructField("name", StringType(), True)
    ]), True),
    StructField("weight_pounds", IntegerType(), True)
])

# Collect all the data in all pages
data = []
for i in range(total_page):
    response = get_data(url, {"page": str(i), "per_page": str(per_page)}, **headers)
    data.extend(response["data"])
    pg_num = i + 1
    progress = (int(pg_num)/int(total_page))*100
    logger.info("Progress: %s" % progress)
    print(f"Page {i+1} of {total_page} - {len(data)}")


# Create DataFrame with data
players_df = sqlc.createDataFrame(data, schema)

# Discovery the columns of the DataFrame with struct type
col_struct = [c for c in players_df.columns if players_df.schema[c].dataType.typeName() == "struct"]

dict_sub_col = {}
# Get all the columns of the struct type
for c in col_struct:
    sub_col = players_df.select(c+".*").columns
    # Create a dictionary with the columns of the struct type
    dict_sub_col.update({c:sub_col})


# Using json struct to explode the struct type
for k, v in dict_sub_col.items():
    for c in v:
        players_df = players_df.withColumn(f'{k}_{c}', col(f'{k}.{c}'))

# Drop the columns of the struct type
for c in col_struct:
    players_df = players_df.drop(c)

# Insert Column date_inserted
players_df = players_df.withColumn("date_inserted", date_format(current_timestamp(), "yyyy-MM-dd"))

# Order dataframe by id
players_df = players_df.orderBy("id")

# Deduplicate dataframe
players_df = players_df.distinct()

# Save data in hive table
db = "dl_bronze"
table = "players_nba"
(
    players_df.write
    .format("parquet")
    .partitionBy("date_inserted")
    .insertInto(f"{db}.{table}", overwrite=False)
)

#==============================================================================================================================
# Get metadata from NBA API
total_page = int(response["meta"]["total_pages"])
current_page = int(response["meta"]["current_page"])
next_page = int(response["meta"]["next_page"])
per_page = int(response["meta"]["per_page"])
total_count = int(response["meta"]["total_count"])

# Create StructType to define schema
schema = StructType([
    StructField("total_page", IntegerType(), True),
    StructField("current_page", IntegerType(), True),
    StructField("next_page", IntegerType(), True),
    StructField("per_page", IntegerType(), True),
    StructField("total_count", IntegerType(), True)
])

# Create DataFrame with metadata
meta_df = sqlc.createDataFrame([(total_page, current_page, next_page, per_page, total_count)], schema)

# Insert Column date_inserted
meta_df = meta_df.withColumn("date_inserted", date_format(current_timestamp(), "yyyy-MM-dd"))

# Save metadata in hive table
db = "dl_bronze"
table = "metadata_nba"
(
    meta_df.write
    .format("parquet")
    .partitionBy("date_inserted")
    .saveAsTable(f"{db}.{table}", mode="append")
)
