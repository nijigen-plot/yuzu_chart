import pydata_google_auth
import pandas as pd
from google.cloud import bigquery
import s3fs
import athena_key

credentials = pydata_google_auth.get_user_credentials(
    ['https://www.googleapis.com/auth/bigquery']
)

client = bigquery.Client(project='voltaic-country-281210', credentials = credentials)

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

print(df)
fs = s3fs.S3FileSystem(key=athena_key.access(), secret=athena_key.secret())
with fs.open('s3://yuzu-charts/chart.csv', 'wb') as f:
    f.write(df.to_csv(index=False).encode())

print('upload completed.')
