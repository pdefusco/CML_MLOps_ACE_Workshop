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
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.types import LongType, IntegerType, StringType, FloatType
from pyspark.sql import functions as F
import dbldatagen as dg
import dbldatagen.distributions as dist
from dbldatagen import FakerTextFactory, DataGenerator, fakerText



class UnlabeledTextGen:

    '''Class to Generate Unlabeled Text Data'''

    def __init__(self, spark):
        self.spark = spark

    def dataGen(self, shuffle_partitions_requested = 8, partitions_requested = 8, data_rows = 1000):

        # setup use of Faker
        FakerTextUS = FakerTextFactory(locale=['en_US'], providers=[bank])

        # partition parameters etc.
        self.spark.conf.set("spark.sql.shuffle.partitions", shuffle_partitions_requested)

        fakerDataspec = (DataGenerator(self.spark, rows=data_rows, partitions=partitions_requested)
                    .withColumnSpec("id", minValue=1, maxValue=data_rows, step=1)
                    .withColumn("text", text=FakerTextUS("address"))
                    )
        df = fakerDataspec.build()
        
        df = df.withColumn("idStr", F.col("id").cast(StringType()))\
            .drop("id")\
            .withColumnRenamed("idStr", "id")
            
        return df
      
import cml.data_v1 as cmldata

# Sample in-code customization of spark configurations
#from pyspark import SparkContext
SparkContext.setSystemProperty('spark.executor.cores', '2')
SparkContext.setSystemProperty('spark.executor.memory', '4g')

CONNECTION_NAME = "go01-aw-dl"
conn = cmldata.get_connection(CONNECTION_NAME)
spark = conn.get_spark_session()
username = "pauldefusco"
        
dg = UnlabeledTextGen(spark)

df = dg.dataGen()

##---------------------------------------------------
##                 SCORE BATCH DATA
##---------------------------------------------------

import mlflow

#Change model path
logged_model = '/home/cdsw/.experiments/8aul-mgvw-shwq-6a2k/dh37-xfvn-dvca-jo9s/artifacts/artifacts'

# Load model as a Spark UDF.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model)

# Predict on a Spark DataFrame.
df.withColumn('label', loaded_model(*column_names)).collect()

##---------------------------------------------------
##                 STORE BATCH AS TEMP TABLE
##---------------------------------------------------

df.createOrReplaceTempView('UNLABELED_TEXT_TEMP_{}'.format(username))

##---------------------------------------------------
##                 ICEBERG MERGE INTO SCORED TEXT
##---------------------------------------------------

ICEBERG_MERGE_INTO = "MERGE INTO MLOPS_WORKSHOP_{0}.LABELED_TEXT_{0} l \
                        USING (SELECT * FROM UNLABELED_TEXT_TEMP_{0}) u \
                        ON l.id = u.id \
                        WHEN MATCHED THEN UPDATE SET l.label = u.label \
                        WHEN NOT MATCHED THEN INSERT VALUES (u.id, u.text, u.label)".format(username)

print("\n")
print("EXECUTING ICEBERG MERGE INTO QUERY")
print(ICEBERG_MERGE_INTO)
spark.sql(ICEBERG_MERGE_INTO)


