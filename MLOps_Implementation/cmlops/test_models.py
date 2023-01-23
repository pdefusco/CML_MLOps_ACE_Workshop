import os, time, json, string
import cmlapi
from cmlapi.rest import ApiException
from pprint import pprint
import random
import logging
from packaging import version
from MLOps_Implementation.cmlops import project_manager, model_manager

# Model Constructor Inputs
base_model_file_path = "/home/cdsw/MLOps_Implementation/models/development_model.sav"
base_model_script_path = "/home/cdsw/MLOps_Implementation/data/X_train.csv"
base_model_training_data_path = "/home/cdsw/MLOps_Implementation/model_endpoint.py"
function_name = "predict"

# Instantiating Project and Model Managers
projManager = project_manager.CMLProjectManager()
modelManager = model_manager.CMLModelManager(base_model_file_path, base_model_script_path, base_model_training_data_path, function_name)

# Retrieving all Model details
Model_AccessKey, Deployment_CRN, Model_CRN, model_endpoint = modelManager.get_all_model_endpoint_details()

# Retrieving Metrics from Model Metrics Store
metrics_df = modelManager.get_model_metrics(self, Model_CRN, Deployment_CRN)
clean_metrics_df = modelManager.unravel_metrics_df(metrics_df)

# Determine if model performs poorly
test_result = modelManager.test_model_performance(clean_metrics_df)

# Trigger Training Job if model performs poorly
train_model_job_id = "iy4m-pu2k-qnz9-rkbh" # manually enter ID from Jobs UI

if test_result == True:
    jobResponse = projManager.get_job(train_model_job_id)
    jobBody = projManager.create_job_body_from_jobresponse(jobResponse)
    jobRun = projManager.run_job(jobBody, train_model_job_id)
