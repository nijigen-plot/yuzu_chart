import pandas as pd
import awswrangler as wr
import boto3

session = boto3.Session(
    aws_access_key_id='',
    aws_secret_access_key='',
    region_name = 'ap-northeast-1'
    )

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

def lambda_handler(event, context):
    df = wr.athena.read_sql_query("SELECT * FROM yuzu_data_raw", database="main_data", boto3_session=session)
    df['category'] = df['name'].apply(lambda x : category_separate(x))
    wr.s3.to_csv(df, 's3://yuzu-charts/chart.csv', index=False, encoding='utf-8-sig', boto3_session=session)
    print('completed chart to S3')
