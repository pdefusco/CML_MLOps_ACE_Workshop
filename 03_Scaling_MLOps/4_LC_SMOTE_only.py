#Hyperparameter tuning
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.tuning import ParamGridBuilder, TrainValidationSplit, CrossValidator
from pyspark.ml.feature import OneHotEncoderEstimator, StringIndexer, VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
from pyspark.mllib.stat import Statistics
from pyspark.ml.linalg import DenseVector
from pyspark.sql import functions as F
import pyspark.sql.functions as sparkf

from LC_Hyperparameter_Tuning_Helper import make_pipeline
import cdsw

import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

def vectorizerFunction(dataInput, TargetFieldName):
    if(dataInput.select(TargetFieldName).distinct().count() != 2):
        raise ValueError("Target field must have only 2 distinct classes")
    columnNames = list(dataInput.columns)
    columnNames.remove(TargetFieldName)
    dataInput = dataInput.select((','.join(columnNames)+','+TargetFieldName).split(','))
    assembler=VectorAssembler(inputCols = columnNames, outputCol = 'features')
    pos_vectorized = assembler.transform(dataInput)
    vectorized = pos_vectorized.select('features',TargetFieldName).withColumn('label',pos_vectorized[TargetFieldName]).drop(TargetFieldName)
    return vectorized

def SmoteSampling(vectorized, k = 5, minorityClass = 1, majorityClass = 0, percentageOver = 200, percentageUnder = 100):
    if(percentageUnder > 100|percentageUnder < 10):
        raise ValueError("Percentage Under must be in range 10 - 100");
    if(percentageOver < 100):
        raise ValueError("Percentage Over must be in at least 100");
    dataInput_min = vectorized[vectorized['label'] == minorityClass]
    dataInput_maj = vectorized[vectorized['label'] == majorityClass]
    feature = dataInput_min.select('features')
    feature = feature.rdd
    feature = feature.map(lambda x: x[0])
    feature = feature.collect()
    feature = np.asarray(feature)
    nbrs = neighbors.NearestNeighbors(n_neighbors=k, algorithm='auto').fit(feature)
    neighbours =  nbrs.kneighbors(feature)
    gap = neighbours[0]
    neighbours = neighbours[1]
    min_rdd = dataInput_min.drop('label').rdd
    pos_rddArray = min_rdd.map(lambda x : list(x))
    pos_ListArray = pos_rddArray.collect()
    min_Array = list(pos_ListArray)
    newRows = []
    nt = len(min_Array)
    nexs = percentageOver//100
    for i in range(nt):
        for j in range(nexs):
            neigh = random.randint(1,k)
            difs = min_Array[neigh][0] - min_Array[i][0]
            newRec = (min_Array[i][0]+random.random()*difs)
            newRows.insert(0,(newRec))
    newData_rdd = spark.sparkContext.parallelize(newRows)
    newData_rdd_new = newData_rdd.map(lambda x: Row(features = x, label = 1))
    new_data = newData_rdd_new.toDF()
    new_data_minor = dataInput_min.unionAll(new_data)
    new_data_major = dataInput_maj.sample(False, (float(percentageUnder)/float(100)))
    return new_data_major.unionAll(new_data_minor)

#pass this from experiments interface
smote_k = 2

spark = SparkSession\
    .builder\
    .appName("LC_Baseline_Model")\
    .config("spark.executor.memory","2g")\
    .config("spark.executor.cores","8")\
    .config("spark.driver.memory","2g")\
    .config("spark.hadoop.fs.s3a.s3guard.ddb.region","us-east-1")\
    .config("spark.yarn.access.hadoopFileSystems","s3a://cdp-cldr-virginia/")\
    .getOrCreate()

#Loading the data
df = spark.read.option('inferschema','true').csv('data/Data_Exploration.csv', header=True)
#df = df.limit(10000)

#Removing unneeded features
remove = ['addr_state', 'earliest_cr_line', 'home_ownership', 'initial_list_status', 'issue_d', 'emp_length',
          'loan_status', 'purpose', 'sub_grade', 'term', 'title', 'zip_code', 'application_type']
df = df.drop(*remove)

#Creating list of categorical and numeric features
cat_cols = [item[0] for item in df.dtypes if item[1].startswith('string')]
num_cols = [item[0] for item in df.dtypes if item[1].startswith('in') or item[1].startswith('dou')]

#We will choose these features for our baseline model:
num_features, cat_features = num_cols, cat_cols

#Dropping nulls
df = df.dropna()
num_features.remove("is_default")

#Transform Dataset
df_model = make_pipeline(df, num_features, cat_features)
input_data = df_model.rdd.map(lambda x: (x["is_default"], DenseVector(x["features"])))
df_pipeline = spark.createDataFrame(input_data, ["is_default", "features"])


scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",withStd=True, withMean=True)
scalerModel = scaler.fit(df_pipeline)
scaledData = scalerModel.transform(df_pipeline)
scaledData = scaledData.drop("features")

#column_names
temp = scaledData.rdd.map(lambda x:[float(y) for y in x['scaledFeatures']]).toDF(num_features + cat_features)

cols = list(df.columns)
cols.remove("is_default")

# This will return a new DF with all the columns + id
df_join = df.withColumn('id', sparkf.monotonically_increasing_id())
temp = temp.withColumn('id', sparkf.monotonically_increasing_id())

df_join = df_join.select('id', 'is_default')
temp = temp.join(df_join, temp.id == df_join.id, 'inner').drop(df_join.id).drop(temp.id)

df_smote = SmoteSampling(vectorizerFunction(temp, 'is_default'), k = smote_k, minorityClass = 1, majorityClass = 0, percentageOver = 400, percentageUnder= 100)

#Stratified Sampling
train = df_smote.sampleBy("label", fractions={0: 0.8, 1: 0.8}, seed=10)
test = df_smote.subtract(train)

#column_names
train.rdd.map(lambda x:[float(y) for y in x['features']]).toDF(cols).toPandas('data/baseline/smote_training.csv')
test.rdd.map(lambda x:[float(y) for y in x['features']]).toDF(cols).toPandas('data/baseline/smote_test.csv')

#Best Model Hyperparameters
#Regularization
cdsw.track_metric("SMOTE_k", smote_k)


