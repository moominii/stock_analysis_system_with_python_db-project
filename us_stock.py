# -*- coding: utf-8 -*-
"""
Created on Wed Mar  5 09:19:49 2025

@author: Admin

getStock() : 실제 주식 데이터를 가져오는 함수. getCompany()에서 호출됨.
getCompany(): 크롤링할 기업의 목록을 데이터베이스로 읽어오는 함수
"""

import yfinance as yf
import pandas as pd
import pymysql

from datetime import datetime, timedelta


# mysql에 연결
host = 'localhost'
user = 'root'
password = 'jiyeoun1'
db_name = 'us_stock'

mysql_conn = pymysql.connect(host=host,
                             user=user,
                             password=password,
                             db=db_name,
                             charset='utf8')

# 실제 주식 데이터를 가져오는 함수 생성. getStock(시작날짜, 종료 날짜)
def getSock(_symbol, _start_date, _end_date):
    # MySQL 데이터베이스와 연결을 설정하고 커서를 생성
    mysql_cur = mysql_conn.cursor()
    
    # 수집하려는 날짜 범위 내 기존 데이터가 있는 경우 삭제하여 중복 저장을 방지.
    mysql_cur.execute("delete from us_stock.stock where date >= %s and date <= %s and symbol = %s", (_start_date, _end_date, _symbol))
    mysql_conn.commit()
    
    try:
        # yf.download() 함수를 사용하여 symbol, start_date, end_date를 설정하여 데이터를 가져옴
        stock_price = yf.download(_symbol, start=_start_date, end = _end_date)
        print(stock_price)
        
        # 가져온 데이터(DataFrame)를 한 행씩 반복하면서 개별 변수에 저장
        for index, row in stock_price.iterrows():
            _date = index.strftime("%Y-%m-%d")
            _open = float(row["Open"])
            _high = float(row["High"])
            _low = float(row["Low"])
            _close =float(row["Close"])
            _adj_close =float(333333)
            _volume = float(row["Volume"])
            
            mysql_cur.execute("insert into us_stock.stock (date, symbol, open, high, low, close, adj_close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (_date, _symbol, _open, _high, _low, _close, _adj_close, _volume))
        mysql_conn.commit()
        
        # 마지막으로 가져온 데이터를 nasdaq_company 테이블에 업데이트하여 다음 크롤링 시 참조할 수 있도록 함
        mysql_cur.execute("update us_stock.nasdaq_company set open = %s, high = %s, low = %s, close = %s, adj_close = %s, volume = %s, last_crawel_date_stock = %s where symbol = %s", (_open, _high, _low, _close, _adj_close, _volume, _date, _symbol))
        mysql_conn.commit()
        
        
    except Exception as e:
        print('error for getStock(): ' + str(e))
        mysql_conn.commit()
        mysql_conn.close()
        
        return {'error for getStock(0) ': str(e)}


# 크롤링한 데이터를 DB로 읽어오는 함수 생성
def getCompany():
    # MySQL 데이터베이스와 연결을 설정하고 커서를 생성
    mysql_cur = mysql_conn.cursor()
    
    # 크롤링이 실행되는 날짜를 기준으로 today 변수를 정의하며, 크롤링 대상 데이터를 정확하게 조회하기 위해 +1일을 더해 설정
    today = datetime.today() + timedelta(days=1)
    
    try:
        # SQL 쿼리를 실행하여 크롤링할 기업 목록을 조회한 후 실행된 쿼리의 결과를 results 변수에 저장
        mysql_cur.execute("select symbol, company_name, ipo_year, last_crawel_date_stock from us_stock.nasdaq_company where is_delete is null;")
        results = mysql_cur.fetchall()
        print(results)
        
        # 조회된 데이터를 반복문을 통해 하나씩 읽으며 변수에 할당
        for row in results:
            _symbol = row[0]
            _company_name = row[1]
            
            # 기업이 nasdaq_company 테이블에 존재하지만 last_crawel_date_stock 값이 NULL인 경우
            # →해당 기업의 데이터를 처음 크롤링하는 것이므로 1970년부터 모든 데이터를 가져옴
            if row[2] is None or row[2] == 0:
                _ipo_year = '1970'
                
            else:
                _ipo_year = row[2]
            
            # last_crawel_date_stock 값이 존재하는 경우 
            # → 마지막 크롤링한 날짜부터 최신 데이터까지 가져오도록 설정
            if row[3] is None:
                _last_crawel_date_stock = str(_ipo_year) + '-01-01'
            else:
                _last_crawel_date_stock = row[3]
                
            print(_symbol)
            if "." in _symbol:
                print(_symbol)
            else:
                if "/" in _symbol:
                    print(_symbol)
                else:
                    getSock(_symbol, _last_crawel_date_stock, today.strftime("%Y-%m-%d"))
    
    except Exception as e:
        print('error for getCompany(): ' + str(e))
        mysql_conn.commit()
        mysql_conn.close()
        
        return {'error for getCompany() ': str(e)}

# 파일 실행할 때 처음 실행될 함수 
if __name__ == '__main__':
    getCompany()





