import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import pandas as pd
import numpy as np
import os

app = dash.Dash()

df = pd.read_csv('campaign_profit.csv')

features = df.columns

y1 = df['blast_profit']
y2 = df['targeted_profit']
x = np.arange(0,1,.1)

app.layout = html.Div([
  
                    dcc.Graph(id='scatterplot',
                  figure = {'data':[
                          go.Scatter(
                          x=x,
                          y=y1,
                          mode='lines',
                          marker = {
                              'size':12,
                              'color': 'rgb(51,204,153)',
                              'symbol':'pentagon',
                              'line':{'width':2}
                          }
                          )],
                  'layout':go.Layout(title='Blast Marketing Campaign',
                                      xaxis = {'title':'Decision Threshold'})}
                    ),
                    dcc.Graph(id='scatterplot2',
                  figure = {'data':[
                          go.Scatter(
                          x=x,
                          y=y2,
                          mode='lines+markers',
                          marker = {
                              'size':12,
                              'color': 'rgb(200,204,53)',
                              'symbol':'pentagon',
                              'line':{'width':2}
                          }
                          )],
                  'layout':go.Layout(title='Targeted Marketing Campaign',
                                      xaxis = {'title':'Decision Threshold'})}
                                        )])

if __name__ == '__main__':
    app.run_server(host='127.0.0.1', port=int(os.environ['CDSW_READONLY_PORT']))