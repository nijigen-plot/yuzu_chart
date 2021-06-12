# import base64

# def hello_pubsub(event, context):
#     """Triggered from a message on a Cloud Pub/Sub topic.
#     Args:
#          event (dict): Event payload.
#          context (google.cloud.functions.Context): Metadata for the event.
#     """
#     pubsub_message = base64.b64decode(event['data']).decode('utf-8')
#     print(pubsub_message)


from google.oauth2 import service_account
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import datetime
from google.cloud import bigquery
import numpy as np
from tqdm import tqdm

credentials = service_account.Credentials.from_service_account_file(
  'cred.json', scopes=['https://www.googleapis.com/auth/cloud-platform']
)
client = bigquery.Client(credentials = credentials, project=credentials.project_id)


def get_data_test():
    search_url = f'https://order.mandarake.co.jp/order/listPage/list?page=1&soldOut=1&keyword=ゆずソフト'
    time.sleep(3)
    site = requests.get(search_url)
    data = BeautifulSoup(site.text, 'html.parser')
    return data

def get_info():
    search_page = 1
    dfs = []
    for i in tqdm(range(20)): # whileのほうが好ましいですがスクレイピングなので上限を設ける
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
    dfs.to_gbq('bqtest.yuzu_chart', project_id='voltaic-country-281210', credentials=credentials, if_exists='append',
                                    table_schema = [{'name':'dt', 'type':'DATE'}])

def main(event, context):
    get_info()