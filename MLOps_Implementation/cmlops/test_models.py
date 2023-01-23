import os, time, json, string
import cmlapi
from cmlapi.rest import ApiException
from pprint import pprint
import random
import logging
from packaging import version
from MLOps_Implementation.cmlops import project_manager, model_manager

base_model_file_path = "/home/cdsw/MLOps_Implementation/models/development_model.sav"
base_model_script_path = "/home/cdsw/MLOps_Implementation/data/X_train.csv"
base_model_training_data_path = "/home/cdsw/MLOps_Implementation/model_endpoint.py"
#model_name = "marketing"
function_name = "predict"

projManager = project_manager.CMLProjectManager()
modelManager = model_manager.CMLProductionModel(base_model_file_path, base_model_script_path, base_model_training_data_path, function_name)
