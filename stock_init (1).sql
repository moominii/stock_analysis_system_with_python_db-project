-- 주식 분석을 위한 데이터베이스 생성
CREATE DATABASE us_stock;

-- 주식 분석을 위한 nasdaq_company 테이블 설정
USE us_stock;

CREATE TABLE nasdaq_company(
symbol VARCHAR(255),
company_name VARCHAR(255),
country VARCHAR (255),
ipo_year INT,
sector VARCHAR(255),
industry VARCHAR(255),
last_crawel_date_stock DATETIME,
is_delete VARCHAR(5),
open DECIMAL(18,2),
high DECIMAL(18,2),
low DECIMAL(18,2),
close DECIMAL(18,2),
adj_close DECIMAL(18,2),
volume BIGINT
);

-- 기본 키를 sumdol 열로 설정
ALTER TABLE nasdaq_company ADD PRIMARY KEY(symbol);

-- 주식 분석을 위한 stock 테이블 설정
USE us_stock;

CREATE TABLE stock(
date DATETIME,
symbol VARCHAR(255),
open DECIMAL(18,2),
high DECIMAL(18,2),
low DECIMAL(18,2),
close DECIMAL(18,2),
adj_close DECIMAL(18,2),
volume BIGINT
);

-- 두 개의 인덱스 생성
CREATE INDEX ix_stock_1 ON stock(date,symbol);
CREATE INDEX ix_stock_2 ON stock(symbol,date);

/*
여기서 사용한 기업 심벌은 사용하는 시점에 따라 해당 기업이 나스닥에 존재 하지 않을 수도 있다.
상장 또는 폐지되는 기업이 수시로 발생하므로 기업 심벌 데이터는 주기적으로 업데이트 해야 한다.
*/
delete from stock;
delete from nasdaq_company;
-- 주식 분석을 위한 기초 데이터 삽입
insert into nasdaq_company (symbol, company_name, ipo_year, sector, industry) values ('TSLA', 'Tesla Inc. Common Stock', 2010, 'Capital Goods', 'K2');
insert into nasdaq_company (symbol, company_name, ipo_year, sector, industry) values ('MSFT', 'Microsoft Corporation Common Stock', 1986, 'Technology', 'K2');
insert into nasdaq_company (symbol, company_name, ipo_year, sector, industry) values ('AMZN', 'Amazon.com Inc. Common Stock', 1997, 'Consumer Services', 'K2');
insert into nasdaq_company (symbol, company_name, ipo_year, sector, industry) values ('AAPL', 'Apple Inc. Common Stock', 1980, 'Technology', 'K2');
insert into nasdaq_company (symbol, company_name, ipo_year, sector, industry) values ('INTC', 'Intel Corporation Common Stock', 1999, 'Technology', 'K2');
insert into nasdaq_company (symbol, company_name, ipo_year, sector, industry) values ('NVDA', 'NVIDIA Corporation Common Stock', 1999, 'Technology', 'K2');
insert into nasdaq_company (symbol, company_name, ipo_year, sector, industry) values ('AMD', 'Advanced Micro Devices Inc. Common Stock', 1999, 'Technology', 'K2');
insert into nasdaq_company (symbol, company_name, ipo_year, sector, industry) values ('META', 'Meta Platforms, Inc.', 2012, 'Technology', 'K2');
insert into nasdaq_company (symbol, company_name, ipo_year, sector, industry) values ('AMPG', 'AMPG, Inc.', 2012, '', 'K2');
insert into nasdaq_company (symbol, company_name, ipo_year, sector, industry) values ('CAR', 'CAR, Inc.', 2012, '', 'K2');
insert into nasdaq_company (symbol, company_name, ipo_year, sector, industry) values ('UAN', 'UAN, Inc.', 2012, '', 'K2');
insert into nasdaq_company (symbol, company_name, ipo_year, sector, industry) values ('BHR', 'BHR, Inc.', 2012, '', 'K2');

SELECT * FROM nasdaq_company;