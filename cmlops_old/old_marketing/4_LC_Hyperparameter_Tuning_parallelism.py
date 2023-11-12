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

df_smote = SmoteSampling(vectorizerFunction(temp, 'is_default'), k = 2, minorityClass = 1, majorityClass = 0, percentageOver = 400, percentageUnder= 100)

#Stratified Sampling
train = df_smote.sampleBy("label", fractions={0: 0.8, 1: 0.8}, seed=10)
test = df_smote.subtract(train)

#Recreating the Logistic Regression model
lr = LogisticRegression(featuresCol='features', labelCol='label', maxIter=10, regParam=0.3, elasticNetParam=0.3)

#Fit the model
lrModel = lr.fit(train)

# We use a ParamGridBuilder to construct a grid of parameters to search over.
# TrainValidationSplit will try all combinations of values and determine best model using
# the evaluator.
paramGrid = ParamGridBuilder()\
    .addGrid(lr.regParam, [0.1, 0.3, 0.5]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.2, 0.5, 0.7, 0.8, 0.9, 1.0])\
    .build()

evaluator = BinaryClassificationEvaluator(labelCol="label")

#We CrossValidate with the paramGrid input above
crossval = CrossValidator(estimator=lr,
                          estimatorParamMaps=paramGrid,
                          evaluator=evaluator,
                          numFolds=3, 
                          parallelism=4) 

# Run cross-validation, and choose the best set of parameters.
cvModel = crossval.fit(train)

#Predict with the Test Set
prediction = cvModel.transform(test)

#Collect Prediction Outcomes
selected = prediction.select("label", "probability", "prediction")

#Collect metrics for further analysis
cvModel.bestModel.summary.recallByThreshold.toPandas().to_csv("data/baseline/CV_results/CV_recall_by_treshold.csv")
cvModel.bestModel.summary.precisionByThreshold.toPandas().to_csv("data/baseline/CV_results/CV_precision_by_treshold.csv")
pd.DataFrame(cvModel.bestModel.summary.objectiveHistory).to_csv("data/baseline/CV_results/CV_objective_history.csv")

#Tracking Metrics
#Accuracy
cdsw.track_metric("BestModelAccuracy", cvModel.bestModel.summary.accuracy)
#Recall
cdsw.track_metric("BestModelRecallLabel0", cvModel.bestModel.summary.recallByLabel[0])
cdsw.track_metric("BestModelRecallLabel1", cvModel.bestModel.summary.recallByLabel[1])
#Precision 
cdsw.track_metric("BestModelPrecisionLabel0", cvModel.bestModel.summary.precisionByLabel[0])
cdsw.track_metric("BestModelPrecisionLabel1", cvModel.bestModel.summary.precisionByLabel[1])

#Best Model Hyperparameters
#Regularization
cdsw.track_metric("BestReguParam", cvModel.bestModel._java_obj.getRegParam())
#Max Iter
cdsw.track_metric("BestMaxIter", cvModel.bestModel._java_obj.getMaxIter())
#Elastic Net
cdsw.track_metric("BestENParam", cvModel.bestModel._java_obj.getElasticNetParam())

