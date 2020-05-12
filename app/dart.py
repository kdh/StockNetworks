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
from zipfile import ZipFile
from io import BytesIO

import dart_fss as dart

from sqlalchemy import create_engine 


engine = create_engine("mssql+pyodbc://DESKTOP-8700K/stock?driver=SQL+Server", echo=False)


crtfc_key='0d4bf392bc8fd6c4c5db87eb977fd7bc030f1ade'

dart.set_api_key(api_key=crtfc_key)

if True:
    df = pd.read_csv('stock_owner.csv')
    #df['stock_code'] = 'A' + df['stock_code']
    #
    #df.drop(columns=['x'], inplace=True)
    #df = df.fillna('')
    print(df.head())
    #df.to_csv('test.csv', index=False, encoding='utf-8-sig')
    df.to_sql('stock_owner', engine, if_exists='replace')
    exit(0)


if False:
    corp_list = dart.get_corp_list()
    corp_list._profile = True

    df_all = None

    df = pd.read_csv('code_list.csv')

    for index, row in df.iterrows():
        stockCode = row['stockCode'][1:]
        print(index, stockCode)

        company = corp_list.find_by_stock_code(stockCode)
        if company != None:
            #print(company.info)

            if df_all is None:
                #print('new df')
                df_all = pd.DataFrame(company.info, index=[4])
            else:
                #print('append df')
                df_all = df_all.append(company.info, ignore_index=True)

            #print(df_all.head())

        else:
            print('empty', stockCode)

        #time.sleep(5)

    df_all.to_csv('company_info.csv')
    exit(0)


if False:

    corp_list = dart.get_corp_list()
    #corp_list.load()
    print('len', len(corp_list))
    #corp_list._profile = True
    print(corp_list)

    for idx, x in enumerate(corp_list.corps):
        print(idx, x)
            
    #for corp in corp_list.corps:
    #    print(corp)
    #    break

    exit(0)
    samsung = corp_list.find_by_stock_code('005930')

    print(samsung.info)
    reports = samsung.search_filings(bgn_de='20100101', pblntf_detail_ty='a001', last_reprt_at='Y')

    for r in reports:
        print(r)
        #r.save()

    exit(0)

    exit(0)

def GetReportList():
    
    url = "http://dart.fss.or.kr/dsab002/search.ax"

    for page in range(1, 20):
        body = f"currentPage={page}&maxResults=100&maxLinks=10&sort=date&series=desc&textCrpCik=&reportNamePopYn=N&textCrpNm=&textPresenterNm=&startDate=20191006&endDate=20200406&finalReport=recent&typesOfBusiness=all&corporationType=all&closingAccountsMonth=all&reportName=%EC%B5%9C%EB%8C%80%EC%A3%BC%EC%A3%BC%EB%93%B1%EC%86%8C%EC%9C%A0%EC%A3%BC%EC%8B%9D%EB%B3%80%EB%8F%99%EC%8B%A0%EA%B3%A0%EC%84%9C"
        filename = f'./data/dart/최대주주등소유주식변동신고서_page_{page}.html'
        print(body)

        if False:
            r = requests.post(url, data=body)

            soup = BeautifulSoup(r.content, 'html.parser', from_encoding='utf-8')
            print(soup.prettify())

            with open(filename, 'w', -1, encoding='UTF8') as f:
                f.write(soup.prettify())

        if True:    
            request = urllib.request.Request(url)
            response = urllib.request.urlopen(request, data=body.encode("utf-8"))
            rescode = response.getcode()

            if(rescode==200):
                response_body = response.read()
                html = response_body.decode('utf-8')
                print(json)
                with open(filename, 'w', -1, encoding='UTF8') as f:
                    f.write(html)        
                
                print("saved")
                #print(json)
            else:
                print("Error Code:" + rescode)

        time.sleep(1)

def GetReport(rcpNo):
    print(rcpNo)

    try:
        url = f'https://opendart.fss.or.kr/api/document.xml?crtfc_key={crtfc_key}&rcept_no={rcpNo}'
        r = requests.get(url)

        zipfile = ZipFile(BytesIO(r.content))
        zipfile.extractall('./data/dart/')
        zipfile.close()
    except Exception as e:
        print('error', e)

def ParsePageFile(page):
    filename = f'./data/dart/최대주주등소유주식변동신고서_page_{page}.html'
   
    rcp_list = []
    with open(filename, 'r', encoding='utf-8') as f:
        bs = BeautifulSoup(f, 'html.parser')

        ll = bs.select('td > a')
        for a in ll:
            id = a['id'][2:]
            #print(id)
            rcp_list.append(id)

            f = f'./data/dart/{id}.xml'
            if os.path.isfile(f):
                pass
            else:
                print('not found', id)
                GetReport(id)
    pass

def ParseAllPage():
    for page in range(7, 20):
        print('page', page)
        ParsePageFile(page)

#def ParseReport(rcpNo):
def ParseReport(filename):
    #filename = f'./data/dart/{rcpNo}.xml'
    #with open(filename, 'r', encoding='utf-8') as f:
    with open(filename, 'r') as f:
        bs = BeautifulSoup(f, 'html.parser')

        table0 = bs.find(id='XFormD1_Form0_Table0')
        tr = table0.select('tr')[0]
        td_list = tr.select('td')
        stockName = td_list[1].text.strip()
        stockName = re.sub(' +', ' ', stockName.replace('\n', ' '))
        stockCode = td_list[3].text.strip()
        stockCode = f'A{stockCode}'
        #print(stockName, stockCode)

        if False and len(stockCode) != 7:
            print('invalid stockCode', stockCode)
            print(td_list[3].text)
            return [0, None]

        people_list = []
        #table1 = bs.find(id='XFormD1_Form0_RepeatTable1')
        table_list = bs.select('table')
        table1 = table_list[len(table_list)-1]
                        
        tr_list = table1.select('tr')
        for tr in tr_list:
            #print(tr)
            td_list = tr.select('td')

            if 17 == len(td_list):
                # name, idnum, volume, ratio
                name = td_list[0].text.strip()
                name = re.sub(' +', ' ', name.replace('\n', ' '))

                idnum = td_list[2].text.strip()
                volume = td_list[15].text.strip().replace(',', '')
                ratio = td_list[16].text.strip()

                #print(name, idnum, volume, ratio)
                people_list.append([stockCode, stockName, name, idnum, volume, ratio])
                #for td in td_list:
                #    print(td.text)

        df = pd.DataFrame(data=people_list, columns=['stockCode', 'stockName',
                                    'name', 'idnum', 'volume', 'ratio'])    

        #df.to_sql('stock_owner', engine, if_exists='append')
        #print(df)

        return [stockCode, df]

def ParseAllReport(path):
    df_all = pd.DataFrame(columns=['stockCode', 'stockName', 'name', 'idnum', 'volume', 'ratio'])

    stock_list = os.listdir(path)
    stock_list.reverse()

    #for file in os.listdir(path):
    for file in stock_list:
        if file.endswith(".xml"):
            filepath = os.path.join(path, file)
            #print(file)
            [stockCode, df] = ParseReport(filepath)

            if stockCode != 0:
                if stockCode in stock_list:
                    print('ignore stockCode', stockCode)
                    pass
                else:
                    stock_list.append(stockCode)
                    df_all = pd.concat([df_all, df])

    #print(df_all)
    
    print(df_all)
    print(len(stock_list))
    df_all.to_csv('stock_owner.csv', index=False, encoding='utf-8-sig')    

    df_all.to_sql('stock_owner', engine, if_exists='append')

#GetReportList()            
#ParseAllReport('./data/dart/')
#ParsePageFile(10)
#ParseReport('./data/dart/20200210800903.xml')

