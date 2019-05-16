#!/usr/bin/env python 
# -*- coding:utf-8 -*-

#!/usr/bin/env python
# -*- coding:utf-8 -*-


import csv
import math
import pandas as pd
import numpy as np
from pandas import DataFrame
from pandas import DataFrame,Series
import threading
import os
import time
import multiprocessing



def get_url(open_file):
    need=[]
    with open(open_file,encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            for j in row:
                j.replace('\t', '')
            last_element = list(row)[-1]
            url = last_element.split("	")[-1]
            row_list = ["  ".join(ele.split("\t")) for ele in row]
            if "//" in url:
                need_dict={}
                url = url.strip()
                need_dict["row_list"]=row_list
                need_dict['url']=url
                need.append(need_dict)
            else:
                continue
    df=pd.DataFrame(need,columns=["row_list","url"])
    df.drop_duplicates(subset='url',keep='first',inplace=True)
    s_list=[]
    for i in df.iloc[:,0]:
        s="".join(i)
        # print(s)
        s_list.append(s+'\n')
    # print(s_list)


    out=open("D:/nudt_work/avaurl/"+str(open_file)[9:],"a",encoding="utf-8")
    out.writelines(s_list)

# get_url("D:/gdelt1/20130401.export.CSV")

#修改path的位置

path="D:/gdelt1"
files=os.listdir(path)

# get_url("D:/gdelt1/20170413.export.CSV")
#写个while循环控制每次的线程数字

a=0
while a<2:
    threads=[]
    for j in files[24*a:24*(a+1)]:
        print(j)
        t=threading.Thread(target=get_url,args=("D:/gdelt1/"+j,))
        threads.append(t)
        print(len(threads))
    if __name__ == "__main__":
        for thr in threads:
            print(thr)
            t.setDaemon(True)
            thr.start()
        thr.join()
        a += 1
        print(a)
        print("all over %s" %time.ctime)
