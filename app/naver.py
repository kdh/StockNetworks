#-*- coding: utf-8 -*-
import os
import sys
import time
import json
import urllib.request
import pandas as pd

client_id = "5DMShweX4BWAFB80YEqs"
client_secret = "FHzlleTZ17"
from sqlalchemy import create_engine 


engine = create_engine("mssql+pyodbc://DESKTOP-8700K/stock?driver=SQL+Server", echo=False)

def RequestNaver(keyword):
    url = "https://openapi.naver.com/v1/datalab/search"
    body = "{\"startDate\":\"2016-01-01\",\"endDate\":\"2020-04-03\",\"timeUnit\":\"date\",\"keywordGroups\":[{\"groupName\":\"" + keyword + "\",\"keywords\":[\"" + keyword + "\"]},{\"groupName\":\"코로나\",\"keywords\":[\"코로나\"]}],\"device\":\"\",\"ages\":[\"4\",\"5\",\"6\",\"7\",\"8\",\"9\",\"10\"],\"gender\":\"\"}"
    #body = "{\"startDate\":\"2017-01-01\",\"endDate\":\"2017-04-30\",\"timeUnit\":\"month\",\"keywordGroups\":[{\"groupName\":\"한글\",\"keywords\":[\"한글\",\"korean\"]},{\"groupName\":\"영어\",\"keywords\":[\"영어\",\"english\"]}],\"device\":\"pc\",\"ages\":[\"1\",\"2\"],\"gender\":\"f\"}";

    filename = "../data/{}.json".format(keyword)
    #print(body)
    print(filename)

    #return

    request = urllib.request.Request(url)
    request.add_header("X-Naver-Client-Id", client_id)
    request.add_header("X-Naver-Client-Secret",client_secret)
    request.add_header("Content-Type","application/json")
    response = urllib.request.urlopen(request, data=body.encode("utf-8"))
    rescode = response.getcode()

    if(rescode==200):
        response_body = response.read()
        json = response_body.decode('utf-8')
        with open(filename, 'w', -1, encoding='UTF8') as f:
            f.write(json)        
        
        print("saved")
        #print(json)
    else:
        print("Error Code:" + rescode)

def ReadFile(keyword):
    filename = "../data/{}.json".format(keyword)
    #print(body)
    print(filename)    

    with open(filename, 'rt', encoding='UTF8') as json_file:
        json_data = json.load(json_file)
        #print(json_data)

        startDate = json_data["startDate"]
        endDate = json_data["endDate"]
        results = json_data["results"]

        list0 = []
        
        #for r in results:
        r = results[0]
        #print(r['title'])
        data = r['data']
        for d in data:
            date = int(d['period'].replace('-', ''))
            ratio = float(d['ratio'])
            if ratio < 1:
                ratio = 0
            #print(keyword, date, ratio)
            else:
                list0.append([keyword, date, ratio])

        if 0 < len(list0):
            df = pd.DataFrame(list0, None, columns=['theme', 'logDate', 'ratio'])
            print(df.head())
            df.to_sql('theme_search', engine, if_exists='append', index=False)
            print('\n')
        else:
            print('no data')


def LoadList(filename):
    f = open(filename, 'rt', encoding='UTF8')
    lines = f.readlines()

    for i in range(0, len(lines)):
    #for line in lines:
        line = lines[i]
        keyword = line.strip()
        print(keyword)
        
        #RequestNaver(keyword)
        ReadFile(keyword)
        #time.sleep(1)

        
#RequestNaver('코로나')
LoadList("../data/theme.csv")
#ReadFile('코로나')