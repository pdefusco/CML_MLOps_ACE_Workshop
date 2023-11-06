#****************************************************************************
# (C) Cloudera, Inc. 2020-2023
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

import os
import numpy as np
import pandas as pd
from datetime import datetime
from pyspark.sql.types import LongType, IntegerType, StringType, FloatType
from pyspark.sql import functions as F
import dbldatagen as dg
import dbldatagen.distributions as dist
from dbldatagen import FakerTextFactory, DataGenerator, fakerText

class LabeledTextGen:

    '''Class to Generate Text Data'''

    def __init__(self, spark):
        self.spark = spark

    def dataGen(self, shuffle_partitions_requested = 8, partitions_requested = 8, data_rows = 10000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumnSpec("id", minValue=1, maxValue=data_rows, step=1)
                    .withColumn("text", text=FakerTextUS("address"))
                    .withColumn("label", "string", values=["0", "1"],random=True)
                    )
        df = fakerDataspec.build()
                
        df = df.withColumn("idStr", F.col("id").cast(StringType()))\
            .drop("id")\
            .withColumnRenamed("idStr", "id")
     
        df = df.withColumn("labelStr", F.col("label").cast(FloatType()))\
            .drop("label")\
            .withColumnRenamed("labelStr", "label")
        
        return df
      
import cml.data_v1 as cmldata

# Sample in-code customization of spark configurations
#from pyspark import SparkContext
#SparkContext.setSystemProperty('spark.executor.cores', '1')
#SparkContext.setSystemProperty('spark.executor.memory', '2g')

CONNECTION_NAME = "go01-aw-dl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()
username = "pauldefusco"
        
dg = LabeledTextGen(spark)

df = dg.dataGen()

#---------------------------------------------------
#       SQL CLEANUP: DATABASES, TABLES, VIEWS
#---------------------------------------------------
print("JOB STARTED...")
spark.sql("DROP DATABASE IF EXISTS MLOPS_WORKSHOP_{} CASCADE".format(username))

##---------------------------------------------------
##                 CREATE DATABASES
##---------------------------------------------------
spark.sql("CREATE DATABASE IF NOT EXISTS MLOPS_WORKSHOP_{}".format(username))

##---------------------------------------------------
##                 STORE AS ICEBERG TABLE
##---------------------------------------------------
df.writeTo("MLOPS_WORKSHOP_{0}.LABELED_TEXT_{0}".format(username)).create()


