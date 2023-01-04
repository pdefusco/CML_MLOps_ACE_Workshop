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


!pip3 install -r requirements.txt

import cdsw, time, os, random, json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from cmlbootstrap import CMLBootstrap
import seaborn as sns
import copy
import cmlapi
from src.api import ApiUtility
from sklearn.metrics import classification_report
from __future__ import print_function
import cmlapi
from cmlapi.rest import ApiException
from pprint import pprint

### MODEL WARMUP ###

df = pd.read_csv("/home/cdsw/data/data.csv")

df = df.sample(2500)

# You can access all models with API V2

client = cmlapi.default_client()

project_id = os.environ["CDSW_PROJECT_ID"]

# You can use an APIV2-based utility to access the latest model's metadata. For more, explore the src folder
apiUtil = ApiUtility()

Model_AccessKey = apiUtil.get_latest_deployment_details_allmodels()["model_access_key"]
Deployment_CRN = apiUtil.get_latest_deployment_details_allmodels()["latest_deployment_crn"]

# Get the various Model Endpoint details
HOST = os.getenv("CDSW_API_URL").split(":")[0] + "://" + os.getenv("CDSW_DOMAIN")
model_endpoint = (
    HOST.split("//")[0] + "//modelservice." + HOST.split("//")[1] + "/model"
)

# This will randomly return True for input and increases the likelihood of returning
# true based on `percent`
def label_error(item, percent):
    if random.random() < percent:
        return True
    else:
        return True if item == "Yes" else False

df.groupby("conversion")["conversion"].count()
#df = df.astype('str').to_dict('records')

# Create an array of model responses.
response_labels_sample = []

# Run Similation to make 1000 calls to the model with increasing error
percent_counter = 0
percent_max = len(df)

for record in json.loads(df.astype("str").to_json(orient="records")):
    print("Added {} records".format(percent_counter)) if (
        percent_counter % 50 == 0
    ) else None
    percent_counter += 1
    no_approve_record = copy.deepcopy(record)
      
    # **note** this is an easy way to interact with a model in a script
    response = cdsw.call_model(Model_AccessKey, no_approve_record)
    response_labels_sample.append(
        {
            "uuid": response["response"]["uuid"],
            "final_label": label_error(record["conversion"], percent_counter / percent_max),
            "response_label": response["response"]["prediction"],
            "timestamp_ms": int(round(time.time() * 1000)),
        }
    )

#{
#    "model_deployment_crn": "crn:cdp:ml:us-west-1:8a1e15cd-04c2-48aa-8f35-b4a8c11997d3:workspace:f5c6e319-47e8-4d61-83bf-2617127acc36/d54e8925-a9e1-4d1f-b7f1-b95961833eb6",
#    "prediction": {
#        "input_data": "{'acc_now_delinq': '1.0', 'acc_open_past_24mths': '2.0', 'annual_inc': '3.0', 'avg_cur_bal': '4.0', 'funded_amnt': '5.0'}",
#        "prediction": "0.0"
#    },
#    "uuid": "7e3b3373-4487-4788-8b63-0ef8d9e9fa5c"
#}
    
# The "ground truth" loop adds the updated actual label value and an accuracy measure
# every 100 calls to the model.
for index, vals in enumerate(response_labels_sample):
    print("Update {} records".format(index)) if (index % 50 == 0) else None
    cdsw.track_delayed_metrics({"final_label": vals["final_label"]}, vals["uuid"])
    if index % 100 == 0:
        start_timestamp_ms = vals["timestamp_ms"]
        final_labels = []
        response_labels = []
    final_labels.append(vals["final_label"])
    response_labels.append(float(vals["response_label"]["prediction"]))
    if index % 100 == 99:
        print("Adding accuracy metric")
        end_timestamp_ms = vals["timestamp_ms"]
        accuracy = classification_report(
            [float(i) for i in final_labels], response_labels, output_dict=True
        )["accuracy"]
        cdsw.track_aggregate_metrics(
            {"accuracy": accuracy},
            start_timestamp_ms,
            end_timestamp_ms,
            model_deployment_crn=Deployment_CRN,
        )