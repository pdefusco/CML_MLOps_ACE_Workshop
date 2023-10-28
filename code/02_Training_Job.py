# ###########################################################################
#
#  CLOUDERA APPLIED MACHINE LEARNING PROTOTYPE (AMP)
#  (C) Cloudera, Inc. 2021
#  All rights reserved.
#
#  Applicable Open Source License: Apache 2.0
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
# ###########################################################################

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

# Retraining Model
X, y = modelManager.unravel_metrics_df(metrics_df)
loaded_model_file = modelManager.load_latest_model_version(model_dir="/home/cdsw/MLOps_Implementation/models")
loaded_model_clf = modelManager.train_latest_model_version(loaded_model_file, X, y)

# Store latest Model
modelManager.store_latest_model_version(loaded_model_clf, model_dir="/home/cdsw/MLOps_Implementation/models")
