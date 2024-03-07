# Code for crawling adjusted stock prices for all stocks, not just for Samsung electronics as seen in SamsungElec_adPrice.py
import pymysql
from sqlalchemy import create_engine
import pandas as pd
from datetime import date
from dateutil.relativedelta import relativedelta
import requests as rq
import time
from tqdm import tqdm
from io import BytesIO
import os

# Connect to DB
DB_USER = os.getenv("DB_USER")
DB_PASSWD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWD}@{DB_HOST}:3306/{DB_NAME}')
con = pymysql.connect(user=DB_USER,
                      passwd=DB_PASSWD,
                      host=DB_HOST,
                      db=DB_NAME,
                      charset='utf8')
mycursor = con.cursor()

# Get ticker list
ticker_list = pd.read_sql("""
select * from kor_ticker
where 기준일 = (select max(기준일) from kor_ticker)
and 종목구분 = '보통주'; 
""", con=engine)

# Save data to DB
query = """
    insert into kor_price (날짜, 시가, 고가, 저가, 종가, 거래량, 종목코드)
    values (%s, %s, %s, %s, %s, %s, %s) as new
    on duplicate key update
    시가 = new.시가, 고가 = new.고가, 저가 = new.저가,
    종가 = new.종가, 거래량 = new.거래량;
"""

# List variable in case of erros
error_list = []

# Receive and save all stock data to DB
for i in tqdm(range(0, len(ticker_list))):
    # select ticker
    ticker = ticker_list['종목코드'][i]
    
    # start and end dates
    fr = (date.today() + relativedelta(years=-5)).strftime("%Y%m%d")
    to = (date.today()).strftime("%Y%m%d")
    
    try:
    
        url = f'''https://m.stock.naver.com/front-api/v1/external/chart/domestic/info?symbol={ticker}&requestType=1
        &startTime={fr}&endTime={to}&timeframe=day'''
        
        # Get data
        data = rq.get(url).content
        data_price = pd.read_csv(BytesIO(data))
        
        # Data cleansing
        price = data_price.iloc[:, 0:6]
        price.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량']
        price = price.dropna()
        price['날짜'] = price['날짜'].str.extract('(\d+)')
        price['날짜'] = pd.to_datetime(price['날짜'])
        price['종목코드'] = ticker
        
        # Save to DB
        args = price.values.tolist()
        mycursor.executemany(query, args)
        con.commit()
        
    except:
        # if an error occurs, save ticker to error_list
        print("Error for " + ticker)
        error_list.append(ticker)
    
    # Precent infinite crawling abuse
    time.sleep(2)

# Disconnect DB
engine.dispose()
con.close()