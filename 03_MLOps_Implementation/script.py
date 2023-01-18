import os
import cmlapi
import pandas as pd
import numpy as np
from cmlops import cmlops_lib

base_model_file_path = "/home/cdsw/models/"
base_model_script_path = base_model_file_path + "development_model.sav"
base_model_training_data_path = "/home/cdsw/data/X_train.csv"
project_id = os.environ["CDSW_PROJECT_ID"]
model_name =
function_name = "predict"

cmlPipeline = ProductionModelPipeline(base_model_file_path, base_model_script_path, base_model_training_data_path, project_id, model_name, function_name)
