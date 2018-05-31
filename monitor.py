from elasticsearch import Elasticsearch
import datetime,time
from pymongo import MongoClient
from config import *

def Counter(entity,name,value,n):
    es=Elasticsearch(ELASTICSEARCH_URL)
    delta=datetime.timedelta(hours=n)
    data={
        "entity":entity,
        "name":name,
        "value":value,
        "timestamp":(datetime.datetime.now()-delta).strftime("%Y-%m-%dT%H:%M:%S.000+0800"),
        "inserttime":datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+0800")
    }
    es.index(index='counter',doc_type='counter',body=data)

def get_counts():
    client=MongoClient(host=MONGODB_URL,port=MONGODB_PORT)
    for i in MONGODB_NAME:
        db=client[i]
        for j in MONGODB_NAME[i]:
            collection=db[j]
            number=collection.count()
            Counter(str(j),str(j)+'_'+'increased',number,n)

def timer():
    sched_time=datetime.datetime(START_YEARS,START_MONTHS,START_DAYS,START_HOURS,START_MINUTES,START_SECONDS)
    timedelta=datetime.timedelta(hours=1)
    now=datetime.datetime.now()
    if str(sched_time-now)[0]=='-':
        print("开始时间已经错过，请调整时间")
    else:
        while True:
            now=str(datetime.datetime.now())[:-7]
            if now==str(sched_time):
               print(sched_time)
               sched_time=str(datetime.datetime.now()+timedelta)[:-7]
               if VALUE == 1:
                    Counter(entity,name,value,n)
               if BASED_ON_MONGODB == 1:
                    mongo_value=get_counts()
                    Counter(entity,name,mongo_value,n)
        time.sleep(3600)

if __name__ == "__main__":
    get_counts()
