import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import datetime
import awswrangler as wr
import boto3

session = boto3.Session(
    aws_access_key_id='',
    aws_secret_access_key='',
    region_name = 'ap-northeast-1'
    )

def get_info():
    search_page = 1
    dfs = []
    for i in range(20): # whileのほうが好ましいですがスクレイピングなので上限を設ける
        search_url = f'https://order.mandarake.co.jp/order/listPage/list?page={str(search_page)}&soldOut=1&keyword=ゆずソフト'
        time.sleep(3)
        site = requests.get(search_url)
        data = BeautifulSoup(site.text, 'html.parser')
        
        # 金額を取得する この際len = 0の場合breakする
        price_data = data.find_all('div', class_='price')
        if len(price_data) == 0:
            print(f'取得終了しました 取得ページは {str(search_page - 1)} ページです')
            break
        else:
            price = []
            for prices in price_data:
                for result in prices.find_all('p'):
                    price.append(''.join(result.get_text().splitlines()).replace('\t','').split('円')[0].replace(',',''))
        

        # 名前とIDを取得する
        title_data = data.find_all('div', class_='title')
        title = []; ids = []
        for titles in title_data:
            for result in titles.find_all('a'):
                try:
                    ids.append(result.attrs['id'])
                except KeyError:
                    ids.append(result.attrs['href'].split('=')[1].split('&')[0])
                title.append(''.join(result.get_text().splitlines()).replace('\t',''))
        ids = ['https://order.mandarake.co.jp/order/detailPage/item?itemCode=' + i for i in ids]
        search_page += 1
        
        # DataFrameを作成する
        df = pd.DataFrame({
            'name': title,
            'price': price,
            'url': ids
        })
        dfs.append(df)
        
    dfs = pd.concat(dfs, axis=0).reset_index(drop=True)
    dfs['price'] = dfs['price'].astype(int) * 1.1
    dfs['price'] = dfs['price'].astype(int)
    dfs['dt'] = datetime.datetime.now().date()
    current_time = str(datetime.datetime.now())
    wr.s3.to_csv(dfs, f's3://quark-sandbox/yuzu_chart_raw/yuzu_chart_{current_time}.csv', index=False,
        boto3_session=session, encoding='utf-8-sig'
    )
    print(f'completed yuzu_chart_{current_time}.csv to S3')

def lambda_handler(event, context):
    get_info()