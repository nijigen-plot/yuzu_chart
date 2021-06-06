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
import s3fs
import athena_key

fs = s3fs.S3FileSystem(key = athena_key.access(), secret=athena_key.secret())
with fs.open('s3://yuzu-charts/chart.csv', 'rb') as f:
    df = pd.read_csv(f)

# df = pd.read_csv('yuzu_chart_csv.csv') ローカルからS3に変更
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
server = app.server # heroku push時は外す




# カテゴリ選択関数
def category_judge(x, df1):
    if x == 'all':
        return df1
    elif x == 'daki':
        return df1[df1['category'] == '抱き枕カバー'].reset_index(drop=True)
    elif x == 'tape':
        return df1[df1['category'] == 'タペストリー'].reset_index(drop=True)
    elif x == 'kuji':
        return df1[df1['category'] == 'くじ特典'].reset_index(drop=True)
    elif x == 'shikishi':
        return df1[df1['category'] == '色紙'].reset_index(drop=True)
    elif x == 'fanbook':
        return df1[df1['category'] == 'ファンブック'].reset_index(drop=True)
    elif x == 'other':
        return df1[df1['category'] == 'その他'].reset_index(drop=True)
    else:
        pass

app.layout = html.Div([
    # タイトル
    html.Div([
        dcc.Markdown(children = '# まんだらけ「ゆずソフト」商品データ'),
        html.A('参照ページはこちら', href='https://order.mandarake.co.jp/order/listPage/list?soldOut=1&keyword=%E3%82%86%E3%81%9A%E3%82%BD%E3%83%95%E3%83%88', target="_blank"),
        html.Br()
    ], style={'display': 'inline-block', 'width': '98%', 'float':'left'}),
    # 時系列と価格一覧に反映させるドロップダウンフィルタ
    html.Div([
        dcc.Markdown(children='- カテゴリフィルタ'),
        dcc.Dropdown(
            id = 'choice_category',
            options=[
                {'value': 'all', 'label': '全体'},
                {'value': 'daki', 'label': '抱き枕カバー'},
                {'value': 'tape', 'label': 'タペストリー'},
                {'value': 'kuji', 'label': 'くじ特典'},
                {'value': 'shikishi', 'label': '色紙'},
                {'value': 'fanbook', 'label': 'ファンブック'},
                {'value': 'other', 'label': 'その他'}
            ],
            value='all',
            clearable=False
        )
    ], style={'display':'inline-block', 'width': '98%', 'float':'left', 'margin-top':'10px'}),
    # 時系列商品数データ
    html.Div([
        dcc.Markdown(children = f'#### 日別取り扱い商品数'),
        dcc.Graph(id = 'time_series_commodity_data')
    ], style={'display':'inline-block', 'width': '98%', 'float':'center'}),
    html.Div([
        # 入荷新商品一覧
        html.Div([
            dcc.Markdown(children = f'#### {today} 入荷商品'),
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
        ], style={'display':'inline-block', 'width': '50%', 'float':'center'}),
        # 価格帯一覧
        html.Div([
            dcc.Markdown(children = f'#### {today} 商品価格一覧'),
            dcc.Graph(id = 'price_box_plot')
        ], style={'display':'inline-block', 'width':'45%', 'float':'right'})
    ], style={'display':'inline-block', 'width': '98%', 'float':'center'})
], style={'background-color': 'rgba(255, 165, 0, 0.05)'})

# 日別取り扱い商品数のフィルタ
@app.callback(
    Output('time_series_commodity_data', 'figure'),
    Output('price_box_plot', 'figure'),
    Input('choice_category', 'value')
)
def update_category(choice_category):
    # 時系列データの方にカテゴリを反映
    time_series_df = category_judge(choice_category, df)
    # 金額データの方にカテゴリを反映
    price_df = category_judge(choice_category, tdf)

    # 時系列データプロット
    fig_a = px.line(time_series_df.groupby(['dt'], as_index=False)['name'].count(), x='dt', y='name')
    fig_a.update_yaxes(title='商品数')
    # 金額データプロット
    fig_b = px.box(price_df, x='price', points='all', hover_data=['name'])
    fig_b.update_xaxes(title='価格')
    fig_b.update_yaxes(showticklabels=False, title='')
    fig_b.update(layout_showlegend=False)

    return fig_a, fig_b

if __name__ ==  '__main__':
    app.run_server(debug=True, port=8052)