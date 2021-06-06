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
print(df)
print('download completed.')
df.to_csv('yuzu_chart_csv.csv', index=False, encoding='utf-8-sig')
