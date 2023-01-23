import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
import numpy as np
import os

def targeted_campaign_profit(cm, mrtk_unit_cost, fixed_campaign_cost, unit_revenue):
    
    #Collecting true and false positives
    fp = cm[0,1]
    tp = cm[1,1]
    
    #Calculating Revenue
    rev = tp*unit_revenue
    #Calculating Costs:
    costs = (fp+tp)*mrtk_unit_cost + fixed_campaign_cost
    #Calculating Profit:
    campaign_profit = rev - costs
    
    return campaign_profit
  
def blast_campaign_profit(y_test, mrtk_unit_cost, fixed_campaign_cost, unit_revenue):
    
    #Collecting true and false positives
    y_accepted = y_test.sum()
    y_denied = y_test.shape[0] - y_accepted
    
    #Calculating Revenue
    rev = y_accepted*unit_revenue
    #Calculating Costs:
    costs = (y_denied+y_accepted)*mrtk_unit_cost + fixed_campaign_cost
    #Calculating Profit:
    campaign_profit = rev - costs
    
    return campaign_profit[0] 

def profit_by_threshold(y_pred_final, y_test, threshold, unit_cost, fixed_cost, unit_revenue):
    
    y_pred = predictions_threshold(y_pred_final, threshold)['pred'].squeeze()
    cm = confusion_matrix(y_test, y_pred)
    
    b_camp_profit = blast_campaign_profit(y_test, unit_cost, fixed_cost, unit_revenue)
    t_camp_profit = targeted_campaign_profit(cm, unit_cost, fixed_cost, unit_revenue) 
    
    return b_camp_profit, t_camp_profit

def draw_profit_curves(y_pred_final, y_test, unit_cost, fixed_cost, unit_revenue):
    
    curves_df = []
    
    for threshold in np.arange(0,1,.1):
        
        b_camp_profit, t_camp_profit = profit_by_threshold(y_pred_final, y_test, threshold, 
                                                           unit_cost, fixed_cost, unit_revenue)
        
        curves_df.append((b_camp_profit, t_camp_profit, threshold))
    
    return pd.DataFrame(curves_df, columns=['blast_profit', 'targeted_profit', 'threshold'])

###################################### DASH APP ##############################################

app = dash.Dash()

df = pd.read_csv('campaign_profit.csv')

features = df.columns

app.layout = html.Div([
  
                    dcc.Input(id='unit_cost', value=5, type='number'),
                    #dcc.Input(id='campaign_cost', value='Campaign Cost', type='number'),
                    #dcc.Input(id='unit_revenue', value='Unit Revenue', type='number'),
                    
                    dcc.Graph(id='scatterplot'),
                    dcc.Graph(id='scatterplot2')

], style={'padding':10})

@app.callback(
    Output(component_id='scatterplot', component_property='figure'),
    [Input(component_id='unit_cost', component_property='value')])
def update_graph_a(unit_cost):
  
    y_test = pd.read_csv('y_test.csv')
    y_pred_final = y_pred_final.read_csv('y_pred_final.csv')
    temp_df = draw_profit_curves(y_pred_final, y_test, unit_cost, 20, 300)
    
    y = temp_df['blast_profit']
    x = np.arange(0,1,.1)
    
    return {
        'data':[
              go.Scatter(
              x=np.arange(0,1,.1),
              y=temp_df['blast_profit'],
              mode='lines',
              marker = {
                  'size':12,
                  'color': 'rgb(51,204,153)',
                  'symbol':'pentagon',
                  'line':{'width':2}
              }
              )],
      'layout':
              go.Layout(title='Blast Marketing Campaign',
                          xaxis = {'title':'Decision Threshold'})
    }
  
@app.callback(
    Output(component_id='scatterplot2', component_property='figure'),
    [Input(component_id='unit_cost', component_property='value')])
def update_graph_b(unit_cost):
  
    y_test = pd.read_csv('y_test.csv')
    y_pred_final = y_pred_final.read_csv('y_pred_final.csv')
    temp_df = draw_profit_curves(y_pred_final, y_test, unit_cost, 20, 300)
    
    y = temp_df['targeted_profit']
    x = np.arange(0,1,.1)
  
    return {'data':[
            go.Scatter(
            x=np.arange(0,1,.1),
            y=temp_df['targeted_profit'],
            mode='lines+markers',
            marker = {
                'size':12,
                'color': 'rgb(200,204,53)',
                'symbol':'pentagon',
                'line':{'width':2}
            }
            )],
    'layout':go.Layout(title='Targeted Marketing Campaign',
                        xaxis = {'title':'Decision Threshold'})
    }


if __name__ == '__main__':  
    app.run_server(host='127.0.0.1', port=int(os.environ['CDSW_READONLY_PORT']))