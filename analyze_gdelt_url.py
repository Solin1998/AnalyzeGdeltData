#!/usr/bin/env python 
# -*- coding:utf-8 -*-
# 1读取url分10分保存到本地avaurl=>
# 2读取url地址将响应问（多线程）将200的写入200url=》
# 3用newspaper（多线程）分析200url中的代码内容
import urllib.request
import csv
import math
import operator
import urllib.request
import urllib.error
import os
import threading
import time
from time import ctime
import newspaper
import queue as Queue


# 1获取文件中的url地址并分析是否为http格式
def get_url(filename):
    # 创立url集合
    url_list = []
    with open(filename) as f:
        reader = csv.reader(f)
        for row in reader:
            url_dict = {}
            id = list(row)[0:9]
            last_element = list(row)[-1]
            url = last_element.split("	")[-1]
            # 已经获取url地址，区分格式是否正确
            if "//" in url:
                # 删除其中的空格
                url = url.strip()
                url_dict["id"] = id
                url_dict["url"] = url
                url_list.append(url)
            else:
                continue
    return url_list
# 将url地址分为10个储存到本地
def res_url(n_file):
    for i in range(n_file):
        out = open("D:/nudt_work/avaurl/" + str(i) + ".csv", "a", encoding="utf-8")
        # write lines 可以提取list
        out.writelines([line + '\n' for line in get_url[i * 2:(i + 1) * 2]])
# reserve=res_url(math.ceil(len(get_url)/20000))
get_url = get_url("D:/nudt_work/ana.csv")
# print(get_url)
#将url储存到本地
res_url=res_url(len(get_url))



# 2多线程返回url的响应码并储存code=200的地址
def analyze_url(fileposition, i):
    ava_list = []
    code_list = []
    reason_list = []
    time_list = []
    connect_list = []
    with open(fileposition + str(i) + ".csv")as f:
        reader = csv.reader(f)
        for row in reader:
            url = row[0]
            try:
                file = urllib.request.urlopen(url)
                if file.getcode() == 200:
                    ava_list.append(url)
                    #并将内容写入到200url的文件
                    out=open("D:/nudt_work/200url/"+str(i)+".csv","a",encoding="utf-8")
                    out.writelines(url)
                    print(url+"200")
            # 定义url错误
            except urllib.error.URLError as e:
                # 不添加错误原因了 如需添加错误原因e就有用处
                # print(e.reason)#测试
                reason_list.append(url)
            # 定义http错误
            except urllib.error.HTTPError as e:
                code_list.append(url)
            # 定义时间错误
            except TimeoutError:
                time_list.append(url)
            except ConnectionResetError:
                connect_list.append(url)
    # 返回所有的错误代码
    return ava_list, code_list, reason_list, time_list, connect_list
# 多线程执行响应内容
try:
    threads = []
    x = 0
    for t in range(0, 11):
        t = threading.Thread(target=analyze_url, args=("D:/nudt_work/avaurl/", x))
        threads.append(t)
        x += 1
except FileNotFoundError:
    pass

if __name__ == "__main__":
    for thr in threads:
        thr.start()
    thr.join()
    print("所有url分析完毕 %s" % ctime())





#第二部分用newspaper多线程分析其中的内容
from newspaper import Article
# avafile为响应为200的网站，positiontit文本，positionkey为关键词
def extbynew(avafile,i,positiontit,positionkey):
    with open(avafile+str(i)+".csv") as f:
        reader=csv.reader(f)
        for row in reader:
            url=row[0]
            print(url)
            news =Article(url)
            news.download()
            news.parse()
            out1 = open(positiontit + "title.csv", 'a', encoding="utf-8")
            out1.writelines(news.title+'\n')
            out2= open(positionkey + "keyword.csv", 'a', encoding="utf-8")
            out2.writelines(str(news.keywords)+'\n')


threads2=[]
x = 0
for t in range(0, 11):
    t = threading.Thread(target=extbynew, args=("D:/nudt_work/200url/",x,"D:/nudt_work/parse/title/","D:/nudt_work/parse/keyword/",))
    threads2.append(t)
    x += 1

if __name__ == "__main__":
    for thr in threads2:
        thr.start()
    thr.join()
    print("所有的文本分析完毕 %s" % ctime())



