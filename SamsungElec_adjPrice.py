'''
It is necessary to record adjusted stock prices for proper backtesting with context. 
The code below crawls adjusted stock prices for Samsung Electronics
'''

from sqlalchemy import create_engine
import pandas as pd
import os

DB_USER = os.getenv("DB_USER")
DB_PASSWD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWD}@{DB_HOST}:3306/{DB_NAME}')
query = """
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker)
    and 종목구분 = '보통주';
"""

ticker_list = pd.read_sql(query, con=engine)
engine.dispose()

from dateutil.relativedelta import relativedelta
import requests as rq
from io import BytesIO
from datetime import date

i = 0
ticker = ticker_list['종목코드'][i]

#Start and End date variables
fr = (date.today() + relativedelta(years=-5)).strftime("%Y%m%d")
to = (date.today()).strftime("%Y%m%d")

url = f'''https://m.stock.naver.com/front-api/v1/external/chart/domestic/info?symbol={ticker}&requestType=1
&startTime={fr}&endTime={to}&timeframe=day'''

data = rq.get(url).content
data_price = pd.read_csv(BytesIO(data))

import re
# Data Cleansing
price = data_price.iloc[:, 0:6]
price.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
price = price.dropna()
price['날짜'] = price['날짜'].str.extract('(\d+)')
price['날짜'] = pd.to_datetime(price['날짜'])
price['종목코드'] = ticker
