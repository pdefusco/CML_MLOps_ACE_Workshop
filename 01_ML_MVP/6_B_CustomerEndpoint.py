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
