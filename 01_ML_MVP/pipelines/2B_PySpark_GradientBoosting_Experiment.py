!pip3 install sklearn

from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import VectorAssembler, StandardScaler, Imputer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.linalg import DenseVector
from pyspark.sql import functions as F

import random
import numpy as np
from sklearn import neighbors
from pyspark.mllib.stat import Statistics

import pandas as pd
import numpy as np

from pyspark.sql import SparkSession

spark = SparkSession\
    .builder\
    .appName("MarketingExperiments")\
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-east-1")\
    .config("spark.yarn.access.hadoopFileSystems","s3a://demo-aws-2/")\
    .getOrCreate()
    
df = spark.sql("SELECT * FROM default.customer_data")

df = df.select(['recency', 'history', 'used_discount', 
         'used_bogo', 'is_referral', 'conversion', 'new'])


#Creates a Pipeline Object including One Hot Encoding of Categorical Features  
def make_pipeline(spark_df):        
     
    for c in spark_df.columns:
        spark_df = spark_df.withColumn(c, spark_df[c].cast("double"))
    
    stages = []

    cols = ['recency', 'history', 'used_discount', 
         'used_bogo', 'is_referral', 'new']
    
    #Assembling mixed data type transformations:
    assembler = VectorAssembler(inputCols=cols, outputCol="features")
    stages += [assembler]    
    
    #Scaling features
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures", withStd=True, withMean=True)
    stages += [scaler]
    
    #Gradient Boosting Classifier
    gbt = GBTClassifier(labelCol="conversion", featuresCol="scaledFeatures", maxIter=10)
    stages += [gbt]
    
    #Creating and running the pipeline:
    pipeline = Pipeline(stages=stages)
    pipelineModel = pipeline.fit(spark_df)
    out_df = pipelineModel.transform(spark_df)
    
    return out_df, pipelineModel
  
  
df_model, pipelineModel = make_pipeline(df)

input_data = df_model.rdd.map(lambda x: (x["conversion"], x["prediction"], float(x['probability'][1])))

#Saving predictions to table
predictions = spark.createDataFrame(input_data, ["conversion", "prediction", "probability"])
predictions\
  .write.format("parquet")\
  .mode("overwrite")\
  .saveAsTable(
    'default.campaign_predictions_experiment_gbt'
)

#Saving pipeline to S3:
pipelineModel.write().overwrite().save("s3a://demo-aws-2/datalake/pdefusco/campaign_gbt")

#Experiment Summary
# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(
    labelCol="conversion", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)


import cdsw
cdsw.track_metric("Accuracy", accuracy)
