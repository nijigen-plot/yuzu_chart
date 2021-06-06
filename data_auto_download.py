import pydata_google_auth
import pandas as pd
from google.cloud import bigquery

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
print('download completed.')
df.to_csv('yuzu_chart_csv.csv', index=False, encoding='utf-8-sig')
