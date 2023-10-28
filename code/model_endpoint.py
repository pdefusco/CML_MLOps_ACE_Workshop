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


from sklearn.ensemble import GradientBoostingClassifier
from sklearn.pipeline import Pipeline
import cdsw
import pandas as pd
import numpy as np
import pickle
import os

# Load most up to date model
def load_latest_model_version():

    model_dir = "/home/cdsw/models"
    models_list = os.listdir(model_dir)
    models_dates_list = [model_path.replace(".sav","") for model_path in models_list if "final" in model_path]
    model_dates = [int(i.split('_')[2]) for i in models_dates_list]
    latest_model_index = np.argmax(model_dates)
    latest_model_path = model_dir + "/" + models_list[latest_model_index]

    loaded_model = pickle.load(open(latest_model_path, 'rb'))

    return loaded_model

customer_behavior_model = load_latest_model_version()

#Inputs:
#recency            int64
#history          float64
#used_discount      int64
#used_bogo          int64
#is_referral        int64
#channel           object -> one hot encoded
#offer             object -> one hot encoded

#Target:
#conversion         int64
@cdsw.model_metrics
def predict(data):
    df = pd.DataFrame(data, index=[0])
    #df.columns = ['recency', 'history', 'used_discount', 'used_bogo', 'is_referral', 'channel', 'offer']
    print(df.columns)
    print(df.head())
    df['recency'] = df['recency'].astype(float)
    df['history'] = df['history'].astype(float)
    df['used_discount'] = df['used_discount'].astype(float)
    df['used_bogo'] = df['used_bogo'].astype(float)
    df['is_referral'] = df['is_referral'].astype(float)

    cdsw.track_metric("input_data", df.T.to_dict()[0])

    pred = customer_behavior_model.predict(df)[0]
    cdsw.track_metric("prediction", float(pred))

    return {'input_data' : df.T.to_dict()[0], 'prediction' : pred}

#{
#  "recency": “6”,
#  "history": "329.08",
#  "used_discount": “1”,
#  "used_bogo": "1",
#  "is_referral": "1",
#  "channel": "Web",
#  "offer": "No Offer"
#}

#{
#  "result": "1"
#}
