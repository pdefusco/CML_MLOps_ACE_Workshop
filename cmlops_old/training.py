#-------------------------------------------------------13.0 ML XGBOost-------------------------------------------------------------------------
#train_df=train_df.limit(188123)

from hyperopt import fmin, tpe, Trials, hp
import numpy as np
import mlflow
import mlflow.spark
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml import Pipeline
from xgboost.spark import SparkXGBRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import numpy as np
from mlflow.models.signature import infer_signature
#vec_assembler = VectorAssembler(inputCols=train_df.columns[1:], outputCol="features")

xgb = SparkXGBRegressor(num_workers=1, label_col="price", missing=0.0)
# pipeline = Pipeline(stages=[vec_assembler, xgb])
pipeline = Pipeline(stages=[ordinal_encoder, vec_assembler, xgb])
regression_evaluator = RegressionEvaluator(predictionCol="prediction", labelCol="price")

def objective_function(params):    
    # set the hyperparameters that we want to tune
    max_depth = params["max_depth"]
    n_estimators = params["n_estimators"]

    return rmse


search_space = {
    "max_depth" : hp.choice('max_depth', np.arange(12, 15, dtype=int)),
     "n_estimators": hp.choice('n_estimators', np.arange(50, 80, dtype=int))
}

mlflow.pyspark.ml.autolog(log_models=True,log_datasets=False)
#mlflow.sklearn.autolog(log_models=False,log_datasets=False)
#mlflow.xgboost.autolog(log_models=True)
#mlflow.transformers.autolog(log_models=False)

num_evals = 1
trials = Trials()
best_hyperparam = fmin(fn=objective_function, 
                       space=search_space,
                       algo=tpe.suggest, 
                       max_evals=num_evals,
                       trials=trials,
                       rstate=np.random.default_rng(42))

# Retrain model on train & validation dataset and evaluate on test dataset
with mlflow.start_run():
    best_max_depth = best_hyperparam["max_depth"]
    best_n_estimators = best_hyperparam["n_estimators"]
    estimator = pipeline.copy({xgb.max_depth: best_max_depth, xgb.n_estimators: best_n_estimators})
    #combined_df = train_df.union(test_df) # Combine train & validation together

    pipeline_model = estimator.fit(train_df)
    pred_df = pipeline_model.transform(test_df)
    #signature = infer_signature(test_df, pred_df)
    rmse = regression_evaluator.evaluate(pred_df)
    r2 = regression_evaluator.setMetricName("r2").evaluate(pred_df)

    # Log param and metrics for the final model
    mlflow.log_param("maxdepth", best_max_depth)
    mlflow.log_param("n_estimators", best_n_estimators)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)
    # mlflow.transformers.log_model(pipeline_model,"model",input_example=test_df.select(old_cols_list).limit(1).toPandas())
    mlflow.spark.log_model(pipeline_model ,"model",input_example=test_df.select(old_cols_list).limit(1).toPandas())
    #mlflow.xgboost.log_model(pipeline_model,"model",input_example=test_df.select(old_cols_list).limit(1).toPandas())
    # mlflow.sklearn.log_model(pipeline_model,"model",input_example=test_df.select(old_cols_list).limit(1).toPandas())