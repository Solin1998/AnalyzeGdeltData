#!/usr/bin/env python 
# -*- coding:utf-8 -*-


#批量分割url地址



import csv
import math
import pandas as pd
import numpy as np
import os
import time
import  threading
    #gfile为初始文件位置，n_file提取url位置


def get_url(input_file):
        url_list_id=[]
        with open(input_file,encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                id = str(row[0])[0:9]
                last_element = list(row)[-1]
                url = last_element.split("	")[-1]
                # print(url)
                if "//" in url:
                    url_dict={}
                    #去除空格
                    url = url.strip()
                    url_dict["id"]=id
                    url_dict['url']=url
                    url_list_id.append(url_dict)
                else:
                    continue
                    #将数据分列后去重,保留第一次出现的url
        df=pd.DataFrame(url_list_id,columns=["id","url"])
        #删除url中重复的，并保留第一个出现的url
        s=df.drop_duplicates(subset=['url'],keep='first')
        #转换为数组类型,s
        dataset=np.array(s)
        dataset=list(dataset.tolist())
        print(len(dataset))
        s=map(lambda x:str(x[0])+x[1]+"\n", dataset)
        #wewant就是我们所需的数据
        # print(s)
        wewant=list(s)
        out = open("D:/nudt_work/avaurl/" + str(input_file)[9:], "a", encoding="utf-8")
        out.writelines(wewant)
        # print(wewant)
        # print(len(wewant))
        return wewant

# get_url("D:/gdelt1/20130423.export.CSV")

path = "D:/gdelt1"
files = os.listdir(path)


# 用*a来控制进程数组
a = 0
while a < 100:
    threads = []
    for j in files[10 * a:10 * (a + 1)]:
        print(j)
        t = threading.Thread(target=get_url, args=(
            "D:/gdelt1/" + j,))
        threads.append(t)
    if __name__ == "__main__":
        for thr in threads:
            print(thr)
            t.setDaemon(True)
            thr.start()
        thr.join()
        a += 1
        print("all over %s" % time.ctime)

#
#
# #原始文件中存在大量重复值，前面九位为ID号[9:]为url地址
#如何在保留id的情况下去除重复的url
# 第一种方法是url_list 然后用for循环比较/pass
# 第二种 单独操作数组中的元素 map，然后删除后面一样的还是要循环/pass
#使用np，pd库 用dataframe的API方式去除重复 √
