#
#Copyright (c) 2022 Cloudera, Inc. All rights reserved.
#

!pip3 install -r requirements.txt

# Create the directories and upload data
from cmlbootstrap import CMLBootstrap
from IPython.display import Javascript, HTML
import os
import time
import json
import requests
import xml.etree.ElementTree as ET
import datetime
import subprocess

run_time_suffix = datetime.datetime.now()
run_time_suffix = run_time_suffix.strftime("%d%m%Y%H%M%S")

# Set the setup variables needed by CMLBootstrap
HOST = os.getenv("CDSW_API_URL").split(":")[0] + "://" + os.getenv("CDSW_DOMAIN")
USERNAME = os.getenv("CDSW_PROJECT_URL").split("/")[6]  # args.username  # "vdibia"
API_KEY = os.getenv("CDSW_API_KEY")
PROJECT_NAME = os.getenv("CDSW_PROJECT")

# Instantiate API Wrapper
cml = CMLBootstrap(HOST, USERNAME, API_KEY, PROJECT_NAME)

# Set the STORAGE environment variable
try:
    storage = os.environ["STORAGE"]
except:
    if os.path.exists("/etc/hadoop/conf/hive-site.xml"):
        tree = ET.parse("/etc/hadoop/conf/hive-site.xml")
        root = tree.getroot()
        for prop in root.findall("property"):
            if prop.find("name").text == "hive.metastore.warehouse.dir":
                storage = (
                    prop.find("value").text.split("/")[0]
                    + "//"
                    + prop.find("value").text.split("/")[2]
                )
    else:
        storage = "/user/" + os.getenv("HADOOP_USER_NAME")
    storage_environment_params = {"STORAGE": storage}
    storage_environment = cml.create_environment_variable(storage_environment_params)
    os.environ["STORAGE"] = storage

### Loading Data to Cloud Storage

!hdfs dfs -mkdir -p $STORAGE/datalake/model_factory
!hdfs dfs -copyFromLocal /home/cdsw/data/LoanStats_2015_subset_091322.csv $STORAGE/datalake/model_factory/LoanStats_2015_subset_091322.csv
!hdfs dfs -ls $STORAGE/datalake/model_factory

### Creating Base Table

from pyspark.sql import SparkSession
from pyspark.sql.functions import *

#create version without iceberg extension options for CDE
spark = SparkSession.builder\
  .appName("0.2 - Batch Load") \
  .config("spark.kerberos.access.hadoopFileSystems", os.environ["STORAGE"])\
  .getOrCreate()

#Explore putting GE here to unit test column types
try:
    # Load and parse the data file, converting it to a DataFrame.
    df = spark.read.csv(os.environ["STORAGE"]+'/datalake/model_factory/LoanStats_2015_subset_091322.csv',   
        header=True,
        sep=',',
        nullValue='NA')
    
    df = df.limit(2000)
    
    #Creating table for batch load if not present
    df.writeTo("default.batch_load_table").create()
    
except:
    sparkDF = spark.sql("SELECT * FROM default.batch_load_table")

else:
    sparkDF = spark.sql("SELECT * FROM default.batch_load_table")
    

