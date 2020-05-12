import pandas as pd
import string
from sqlalchemy import create_engine 
import os
from sqlalchemy.exc import ProgrammingError

'''
POSTGRES_USER = os.environ['POSTGRES_USER']
POSTGRES_PASSWORD = os.environ['POSTGRES_PASSWORD']
POSTGRES_DB = os.environ['POSTGRES_DB']
POSTGRES_HOST = os.environ['POSTGRES_HOST']


select stockCode, len(stockcode), stockName, name, replace(idnum, '-', '') as idnum, ratio 
from dart_stockholder where ratio > 0 and len(stockcode) != 7


update dart_stockholder
set stockCode = replace(stockcode, ' ', '0')
where len(stockcode) != 7
and replace(stockcode, ' ', '0') in (select shortcode from TBCA01)

update dart_stockholder
set stockCode = replace(stockcode, 'A', 'A0')
where len(stockcode) != 7
and replace(stockcode, 'A', 'A0') in (select shortcode from TBCA01)



'''

def clean_columns(df):
    df = df.copy()
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '').str.replace('[{}]'.format(string.punctuation), '').str.replace('£','')
    return df


def get_pg_engine():
    #engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:5432/{POSTGRES_DB}')
    engine = create_engine("mssql+pyodbc://DESKTOP-8700K/stock?driver=SQL+Server", echo=False)

    return engine


def read_symbols(limit = 0):
    engine = get_pg_engine()
    assert limit >= 0, 'Limit must be positive'
    assert isinstance(limit, int), 'Limit has to be an integer'
    if limit == 0:
        query = f"SELECT shortcode as symbol, stockname as name FROM TBCA01"
    else:
        query = f"SELECT  TOP ({limit}) shortcode as symbol, stockname as name FROM TBCA01 "

    try:
        df = pd.read_sql(query, engine)
    except ProgrammingError:
        df = None
    
    return df

def read_company():
    engine = get_pg_engine()
    query = f'select stock_code, stock_name, bizr_no as idnum from dart_company'
    df = pd.read_sql(query, engine)
    return df

def read_person():
    engine = get_pg_engine()
    query = f'select name, idnum from dart_stockholder where ratio > 0 and len(idnum) < 10 group by name, idnum'
    df = pd.read_sql(query, engine)
    return df

def read_holder():
    engine = get_pg_engine()
    query = f"select stockCode, stockName, name, idnum, ratio from dart_stockholder where ratio > 0"
    df = pd.read_sql(query, engine)
    return df

def read_stock(limit = 0):
    engine = get_pg_engine()
    assert limit >= 0, 'Limit must be positive'
    assert isinstance(limit, int), 'Limit has to be an integer'
    
    query = f"SELECT shortCode as stockCode, stockname as stockName FROM TBCA01 WHERE MarketKind in ('거래소', '코스닥') and shortCode like '%0' order by shortcode"

    try:
        df = pd.read_sql(query, engine)
    except ProgrammingError:
        df = None
    
    return df

def read_corr(limit = 0):
    engine = get_pg_engine()
    assert limit >= 0, 'Limit must be positive'
    assert isinstance(limit, int), 'Limit has to be an integer'
    
    query = f"SELECT code1, code2, corr_max as corr from stock_corr"

    try:
        df = pd.read_sql(query, engine)
    except ProgrammingError:
        df = None
    
    return df


def read_themes(limit = 0):
    engine = get_pg_engine()

    query = f"SELECT theme as name FROM theme_search group by theme order by theme"

    try:
        df = pd.read_sql(query, engine)
    except ProgrammingError:
        df = None
    
    return df    

def read_table(table_name, limit = 0):
    engine = get_pg_engine()
    assert limit >= 0, 'Limit must be positive'
    assert isinstance(limit, int), 'Limit has to be an integer'
    if limit == 0:
        query = f"SELECT * FROM {table_name}"
    else:
        query = f"SELECT  TOP ({limit}) * FROM {table_name}"

    try:
        df = pd.read_sql(query, engine)
    except ProgrammingError:
        df = None
    
    return df
