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
import warnings
import sys
import xgboost
import mlflow
import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split
import mlflow.sklearn
import logging

mlflow.set_experiment(experiment_name="heart-condition-classifier")

file_url = "https://azuremlexampledata.blob.core.windows.net/data/heart-disease-uci/data/heart.csv"
df = pd.read_csv(file_url)
                 
df["thal"] = df["thal"].astype("category").cat.codes
                 
df["thal"].unique()
  
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
    df.drop("target", axis=1), df["target"], test_size=0.3
)
                 
from xgboost import XGBClassifier

with mlflow.start_run():

    model = XGBClassifier(use_label_encoder=False, eval_metric="logloss")
  
    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    y_pred = model.predict(X_test)

    from sklearn.metrics import accuracy_score, recall_score

    accuracy = accuracy_score(y_test, y_pred)
    recall = recall_score(y_test, y_pred)                 

    print("Accuracy: %.2f%%" % (accuracy * 100.0))
    print("Recall: %.2f%%" % (recall * 100.0))
  
    mlflow.log_param("accuracy", accuracy)
    mlflow.log_param("recall", recall)
    mlflow.xgboost.log_model(model, artifact_path="artifacts", registered_model_name="my_xgboost_model")
  
#Replace Experiment Run ID here:
run = mlflow.get_run("n5rt-lafm-0dkn-uw2t")

pd.DataFrame(data=[run.data.params], index=["Value"]).T
                 
pd.DataFrame(data=[run.data.metrics], index=["Value"]).T
                 
client = mlflow.tracking.MlflowClient()
client.list_artifacts(run_id=run.info.run_id)

file_path = mlflow.artifacts.download_artifacts(
    run_id=run.info.run_id, artifact_path="feature_importance_weight.png"
)
                                
import matplotlib.pyplot as plt
import matplotlib.image as img

image = img.imread(file_path)
plt.imshow(image)
plt.show()