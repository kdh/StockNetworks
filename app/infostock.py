#-*- coding: utf-8 -*-
import os
import sys
import time
import json
import urllib.request
import requests
import pandas as pd
from bs4 import BeautifulSoup
import re
from pathlib import Path
import datetime

def GetTheme(code):
    url = f'http://m.infostock.co.kr/sector/sector_detail.asp?code={code}&mode=w'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser', from_encoding='utf-8')

    print(code)

    filename = f'./data/{code}.html'
    with open(filename, 'w', -1, encoding='UTF8') as f:
        f.write(soup.prettify())

def GetList():
    url = "http://m.infostock.co.kr/sector/sector.asp?mode=w"

    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser', from_encoding='utf-8')

    theme_list = []

    l = soup.select('td > a')
    print(f'theme_length : {len(l)}')
    for a in l:
        s = a['href'].split("'")
        if 3 < len(s):
            #print(s[1], s[3])
            theme_list.append([s[3], s[1]])
            GetTheme(s[3])

            time.sleep(2)

    #print(theme_list)

'''
날짜별 특징 상한가 정보 가져오기
'''
def GetDateTheme(date):
    url = f'http://m.infostock.co.kr/dataBank/spot06.asp?mode=w&SearchField=%B3%AF%C2%A5&searchword={date}'
    r = requests.get(url)
    soup = BeautifulSoup(r.content, 'html.parser', from_encoding='utf-8')

    #print(code)

    filename = f'./data/date/{date}.html'
    with open(filename, 'w', -1, encoding='UTF8') as f:
        f.write(soup.prettify())

'''
전체 날짜 특징 상한가 가져오기
'''
def GetDateThemeList():


    today = datetime.datetime.now()

    today = datetime.datetime.strptime('2019-01-01', '%Y-%m-%d')
    first = datetime.datetime.strptime('2015-01-01', '%Y-%m-%d')    
    #while True:
    while today > first:
        weekday = today.weekday()
        if weekday < 5:
            date = today.strftime("%Y-%m-%d")
            print(today.weekday(), date)

            GetDateTheme(date)
        
            time.sleep(1)

        today = today + datetime.timedelta(-1)

    pass


GetDateThemeList()
exit(0)

def LoadFile_(file):
    code = Path(file).stem
    #file = f'./data/{code}.html'
    print(code)
    
    with open(file, 'r', encoding='utf-8') as f:
        bs = BeautifulSoup(f, 'html.parser')
        #print(bs)
        table = bs.select('table')
        #print(table)
        tr_list = table[1].select('tr')
        name = tr_list[0].select('td')[1].text.strip()
        print(name)
        #return
        
        stock_list = []
        for tr in tr_list:
            td_list = tr.select('td')
            if 2 == len(td_list):
                t = td_list[0].text.replace('\n', '')
                #print(t)
                info = td_list[1].text.strip()
                arr = re.split('\W+', t)
                if 4 == len(arr):
                    stockCode = arr[2]
                    if len(stockCode) == 6:
                        stockCode = f'A{stockCode}'
                        stockName = arr[1]
                        #df.append([code, name, stockCode, stockName, info])
                        info = info.replace('\n', '')
                        stock_list.append([code, name, stockCode, stockName, info])
                        #print(code, name, stockCode, stockName, '--', info)

        #print(len(tr))

        df = pd.DataFrame(data=stock_list, columns=['themeCode', 'themeName', 'stockCode', 'stockName', 'info'])
        #print(df)

        return df

def LoadFile(file):
    code = Path(file).stem
    #file = f'./data/{code}.html'
    print(code)
    
    with open(file, 'r', encoding='utf-8') as f:
        bs = BeautifulSoup(f, 'html.parser')
        #print(bs)
        table = bs.select('table')
        #print(table)
        tr_list = table[1].select('tr')
        name = tr_list[0].select('td')[1].text.strip()
        desc = tr_list[1].select('td')[1].text.strip()
        #print(name, desc)

        issue_list = []
        td_issue = tr_list[2].select('td')[1].text.strip()
        #print(td_issue)

        date = ''
        for line in td_issue.split('\n'):
            t = line.strip()
            if 5 < len(t):
                if len(t) == 12:
                    date = t.replace('(', '').replace(')', '').replace('-', '')                    
                else:
                    issue_list.append([code, name, date, t])
                    #print(date, t)
              
        stock_list = []
        for tr in tr_list:
            td_list = tr.select('td')
            if 2 == len(td_list):
                t = td_list[0].text.replace('\n', '')
                #print(t)
                info = td_list[1].text.strip()
                arr = re.split('\W+', t)
                if 4 == len(arr):
                    stockCode = arr[2]
                    if len(stockCode) == 6:
                        stockCode = f'A{stockCode}'
                        stockName = arr[1]
                        #df.append([code, name, stockCode, stockName, info])
                        info = info.replace('\n', '')
                        stock_list.append([code, name, stockCode, stockName, info])
                        #print(code, name, stockCode, stockName, '--', info)

        #print(len(tr))

        df_stock = pd.DataFrame(data=stock_list, columns=['themeCode', 'themeName', 'stockCode', 'stockName', 'info'])
        df_issue = pd.DataFrame(data=issue_list, columns=['themeCode', 'themeName', 'date', 'info'])
        #print(df)
        #return df

        result = {
            'code': code,
            'desc': desc,
            'df_issue': df_issue,
            'df_stock': df_stock,
        }

        return result        

def LoadDirectory(path):
    df_issue = pd.DataFrame(columns=['themeCode', 'themeName', 'date', 'info'])
    df_stock = pd.DataFrame(columns=['themeCode', 'themeName', 'stockCode', 'stockName', 'info'])

    for file in os.listdir(path):
        if file.endswith(".html"):
            filepath = os.path.join(path, file)
            info = LoadFile(filepath)
            if info != None:
                df_issue = pd.concat([df_issue, info['df_issue']])
                df_stock = pd.concat([df_stock, info['df_stock']])

    #print(df_all)
    print(df_issue)
    #df_all.to_csv('theme_infostock.csv', index=False, encoding='utf-8-sig')
    df_issue.to_csv('theme_issue.csv', index=False, encoding='utf-8-sig')
#GetList()
#GetTheme(64)
#LoadFile(3)
#info = LoadFile('./data/380.html')
#print(info)
LoadDirectory("./data")

