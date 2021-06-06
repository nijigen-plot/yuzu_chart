import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table
import plotly.express as px
import plotly.graph_objects as go
from dash.dependencies import Input, Output, State
import pandas as pd
import datetime
import numpy as np
import os

df = pd.read_csv('yuzu_chart_csv.csv')
# 更新時によってずれるからDataFrameから参照する
today = df['dt'].max()
yesterday = str(pd.to_datetime(today).date() - datetime.timedelta(days=1))
# 昨日はなかった商品群
stock_new_item = pd.concat([df[df['dt'] == today],
           df[df['dt'] == yesterday]], axis=0).drop_duplicates('name', keep=False)
# 今日の商品群
tdf = df[df['dt'] == today]
# カテゴリもこれにつけようか
# 全体・抱き枕・タペストリー・とか諸々

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
app = dash.Dash(__name__, external_stylesheets = external_stylesheets)
server = app.server
fig_a = px.line(df.groupby(['dt'], as_index=False)['name'].count(), x='dt', y='name', title='日別取り扱い商品数')
fig_a.update_yaxes(title='商品数')

fig_b = px.box(tdf, x='price', title='商品価格分布', points='all', hover_data=['name'])
fig_b.update_xaxes(title='価格')
fig_b.update_yaxes(showticklabels=False, title='')
fig_b.update(layout_showlegend=False)



app.layout = html.Div([
    # 一番上 時系列商品数データ
    html.Div([
        dcc.Graph(id = 'time series commodity data',
                 figure = fig_a
                 )
    ], style={'display':'inline-block', 'width': '98%', 'float':'center'}),
    html.Div([
        # 入荷新商品一覧
        html.Div([
            dash_table.DataTable(
                style_data={
                    'whiteSpace':'normal',
                    'height':'auto'
                },
                id = 'stock_new_item_table',
                data=stock_new_item[['name','price']].to_dict('records'),
                columns=[{'name': '商品名', 'id': 'name'}, {'name': '価格', 'id': 'price'}],
                style_cell=dict(textAlign='center', font_size=12),
                style_header=dict(textAlign='center', backgroundColor='bisque', font_size=17)
            )
        ], style={'display':'inline-block', 'width': '45%', 'float':'left'}),
        # 価格帯一覧
        html.Div([
            dcc.Graph(id = 'price_box_plot',
                      figure = fig_b
                     )
        ], style={'display':'inline-block', 'width':'45%', 'float':'right'})
    ], style={'display':'inline-block', 'width': '98%', 'float':'center'})
], style={'background-color': 'rgba(255, 165, 0, 0.05)'})

app.run_server(port=8052, debug=True)