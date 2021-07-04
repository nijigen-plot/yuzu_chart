import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os

str_1 = eval(os.environ.get('str_1'))
credentials = service_account.Credentials.from_service_account_info(str_1, scopes=["https://www.googleapis.com/auth/bigquery"])
client = bigquery.Client(project='voltaic-country-281210', credentials=credentials)
query = """
SELECT * FROM `voltaic-country-281210.bqtest.yuzu_chart`
"""

df = client.query(query).to_dataframe()

def category_separate(x):
    if ('抱き枕') in x:
        return '抱き枕カバー'
    elif ('タペストリ') in x:
        return 'タペストリー'
    elif ('色紙') in x:
        return '色紙'
    elif ('ファンブック') in x:
        return 'ファンブック'
    elif ('くじ') in x:
        return 'くじ特典'
    else:
        return 'その他'
# カテゴリを生成
df['category'] = df['name'].apply(lambda x : category_separate(x))

# actionsディレクトリに出力
df.to_csv('.github/workflows/chart.csv', index=False, encoding='utf-8-sig')
