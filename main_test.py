# -*- coding: utf-8 -*-
"""
Created on Tue Mar  4 15:49:48 2025

@author: Admin

파이썬 크롤러 제작

    설치한 야후 파이낸스 API인 yfinance 를 호출하여 데이터를 수집하고 가공하여, 
    MySQL 서버로 데이터를 저장하는 역할.


작업 순서

1. 파이썬 패키지 임포트.
2. MySQL 접속 정보.
3. 실제 주식 데이터를 가져 오는 함수 생성.
4. 크롤링 할 기업의 목록을 데이터베이스로 읽어 오는 함수 생성.
5. main.py 파일을 실행할 때 처음 실행되는 코드.
"""

### 1. 파이썬 패키지 임포트 ###
# pip install yfinance 
# pip install pymysql

from datetime import datetime, timedelta

import pymysql
import pandas as pd
import yfinance as yf



### 2. MySQL 접속 정보 ###
hostName = '127.0.0.1'
userName = 'root'
password = 'jiyeoun1'
dbName = 'us_stock'

mysql_conn = pymysql.connect(host = hostName, 
                             user = userName, 
                             password = password, 
                             db = dbName)


### 3. 실제 주식 데이터를 가져 오는 함수 생성 : getStock(시작날짜, 종료날짜) ###
# getCompany() 에서 사용

## mysql 데이터베이스와 연결 하는 커서를 연다 .
## 중복된 값을 저장하지 않기위해, 크롤링하려는 날짜의 데이터가 존재 하면 삭제.
## try
    ## yf.download() 를 통해 주식 데이터를 가져와 stock_price 변수에 저장
    # _symbol은 심벌 이름을 할당.
    # _start_date와 _end_date는 수집할 날짜의 시작과 끝 날짜를 할당.
    
    ## 데이터 프레임 형태의 데이터를 각 변수에 저장.
    # stock_price 라는 변수에 데이터 프레임 형태로 저장된 결과를
    # 한 행씩 읽으면서 각 변수에 값을 할당.
    # for
        ## _date ,_open 과 같은 변수에 값을 할당.
        # _date는 데이터의 날짜를 YYYY-MM-DD 형태로 값을 할당.
        
        # 나머지 변수에 오른쪽의 결괏값인
        # 시작가 , 최고가 , 최저가 , 종가 등의 값을 각각 할당.
        # stock 테이블에 크롤링한 데이터 입력.

    # 크롤링한 데이터의 마지막 날짜를 기록하여,
    # 다음 크롤링 시 참고할 수 있도록 nasdaq_company 테이블에 업데이트.

## 예외 발생 시, 실행. 
## except 
    # 오류를 출력 하고 로직을 종료.
    
def getStock(_symbol, _start_date, _end_date):
    mysql_cur = mysql_conn.cursor()
    
    mysql_cur.execute("delete from us_stock.stock where date >= %s and date <= %s and symbol = %s", (_start_date, _end_date, _symbol))
    mysql_conn.commit()
    
    try:
        stock_price = yf.download(_symbol, start=_start_date, end=_end_date)
        print('00' *10)
        print(stock_price)
        print('00' *10)
        
        for index, row in stock_price.iterrows():
            _date = index.strftime("%Y-%m-%d")
            
            _open = float(row["Open"])
            _high = float(row["High"])
            _low = float(row["Low"])
            _close = float(row["Close"])
            _adj_close = float(101309500)
            
            _volume = float(row["Volume"])

            mysql_cur.execute("insert into us_stock.stock (date, symbol, open, high, low, close, adj_close, volume) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (_date, _symbol, _open, _high, _low, _close, _adj_close, _volume))

        mysql_conn.commit()
        
        mysql_cur.execute("update us_stock.nasdaq_company set open = %s, high = %s, low = %s, close = %s, adj_close = %s, volume = %s, last_crawel_date_stock = %s where symbol = %s", (_open, _high, _low, _close, _adj_close, _volume, _date, _symbol))
        mysql_conn.commit()
        
    except Exception as e:
        print ("error for getStock() : " + str(e))
        mysql_conn.commit()
        mysql_conn.close()
        
        return {'error for getStock() ': str(e)}



### 4. 크롤링 할 기업의 목록을 데이터베이스로 읽어 오는 함수 생성 : getCompany() ###
## mysql 데이터베이스와 연결 하는 커서를 연다

## 날짜를 정의하는데,
## 파이썬 크롤러가 실행될 때의 날짜에 + 1 일로 하여 today 라는 변수에 저장

# try:
    ## 매개 변수로 사용된 쿼fl를 실행하는 함수
    
    ## 쿼리를 실행한 코드의 결과를 읽어 오는 함수

    ## 크롤링한 데이터를 목적에 맞는 변수에 데이터 할당. 
    # 이때 , 변수에 할당할 데이터가 존재 하지 않으면
    # else 부분이 실행되어 별도의 데이터가 할당.
    
    ## results 변수에 저장된 데이터를 행 단위로 읽어
        # for
            # _symbol 과 같이 열 이름으로 만든 각각의 변수에 값( 데이터 )을 할당.
            
            # 만약 새로 추가된 심벌로
            # nasdaq_company 테이블의 last_crawel_date_stock 열의 값이 NULL 일 경우
            # 크롤링 했던 기록이 없다는 뜻 이므로
                # if
                    # 과거의 모든 데이터를 가져 오기 위해 1970 년 부터 읽어 올 수 있도록 코드를 작성.
                # 크롤링 기록이 있으면 
                # 마지막 크롤링한 날짜부터 최근 날짜까지의 데이터를 조회할 수 있도록..

## 예외 발생 시, 실행.   
# except

def getCompany():
    mysql_cur = mysql_conn.cursor()
    
    today = datetime.today() + timedelta(days=1)
    
    try:
        mysql_cur.execute("select symbol, company_name, ipo_year, last_crawel_date_stock from us_stock.nasdaq_company where is_delete is null;")
        results = mysql_cur.fetchall()
        print('=' * 10) 
        print(results) 
        print('=' * 10) 
        
        for row in results:
            _symbol = row[0]
            _company_name = row[1]
            
            if row[2] is None or row[2] == 0:
                _ipo_year = '1970'
            else:
                _ipo_year = row[2]

            if row[3] is None:
                _last_crawel_date_stock = str(_ipo_year) + '-01-01'
            else:
                _last_crawel_date_stock = row[3]
                
            print('-' * 10) 
            print (_symbol)    
            print('-' * 10) 
            
            if "." in _symbol:
                print('//' * 10) 
                print(_symbol)
                print('//' * 10)
            else:
                if "/" in _symbol:
                    print('$' * 10)
                    print(_symbol)
                    print('$' * 10)
                else:
                    print('@' * 10)
                    print(_last_crawel_date_stock)
                    print('@' * 10)
                    getStock(_symbol, _last_crawel_date_stock, today.strftime("%Y-%m-%d"))
                    
    except Exception as e:
        print ("error for getCompany(): " + str(e))
        mysql_conn.commit()
        mysql_conn.close()
        
        return {'error for getCompany() ': str(e)}               
                 
### 5. main.py 파일을 실행할 때 처음 실행되는 코드. ###
# getCompany( )를 정의하는 코드. 
if __name__ == '__main__':
# execute only if run as a script
    getCompany()



























