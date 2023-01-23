from sklearn.ensemble import GradientBoostingClassifier
from sklearn.pipeline import Pipeline
import cdsw
import pandas as pd
import pickle

customer_behavior_model = pickle.load(open('/home/cdsw/final_model.sav', 'rb'))
df_test = pd.read_csv("/home/cdsw/data/batch_data_2.csv")

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

#@cdsw.model_metrics
def batch_scoring(df):

    df_pred = customer_behavior_model.predict(df)

    df["score"] = df_pred

    #cdsw.track_metric("input_data", df.T.to_dict()[0])
    #cdsw.track_metric("prediction", float(pred))

    df.to_csv("/home/cdsw/data/batch_pred_2.csv", index=False)
    cdsw.track_metric("prediction", df["score"])

    #return {'prediction_df' : df_pred}


batch_scoring(df_test)
