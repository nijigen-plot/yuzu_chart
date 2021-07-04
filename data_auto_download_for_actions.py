import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import os
import base64
import s3fs

# githubのsecretsから鍵を持ってくる
str_1 = eval(base64.b64decode((os.environ.get('str_1'))))
athena_a = os.environ.get('athena_access')
athena_s = os.environ.get('athena_secret')

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

# S3へアップロード
fs = s3fs.S3FileSystem(key=athena_a, secret=athena_s)
with fs.open('s3://yuzu-charts/chart.csv', 'wb') as f:
    f.write(df.to_csv(index=False).encode())
