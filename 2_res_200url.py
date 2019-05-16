#!/usr/bin/env python 
# -*- coding:utf-8 -*-
import urllib.request
import csv
import math
import urllib.request
import urllib.error
import threading
import datetime
from time import ctime
import newspaper
from newspaper import Article
import time
import os


# import socket
# socket.setdefaulttimeout(4)

# 分类：
# 1可以访问可以解析
# 2可以访问不能解析
# 3显示错误，但能访问，能解析
# 4显示错误，不能访问，不能解析，用id作为标识
# 问题：错误1的编码，断点虫爬，响应时间跳过
# 逻辑：先看是否能解析，再添加
# error4：可以开vpn访问,error1 forriben错误基本可以访问


# filepos=解析文件文档,i=解析个数,textpos=文本地址,unparse=不能解析文本地址

# 读取已经爬过的文件，如果存在就不进行爬虫

# 执行一次即ava_url,第一个有重复数据,找出重复数据的来源.写成程序，第二次运行的时候执行第一个函数


def analyze_url(filepos, i, textpos, unparse):
    ava_list = []
    code_list = []
    reason_list = []
    time_list = []
    connect_list = []
    id_list = []

    # 将已经爬取的id储存到1.csv
    out = open("D:/nudt_work/has/1.csv", "a+", encoding="utf-8")
    with open("D:/nudt_work/has/1.csv") as f1:
        reader1 = csv.reader(f1)
        for row1 in reader1:
            record_id = row1[0]
            record_id = record_id.strip()
            id_list.append(record_id)
            print(len(id_list))
    with open(filepos)as f:
        reader = csv.reader(f)
        for row in reader:
            url = str(row[0])[9:]
            url = url.strip()
            # 用id标识url
            id = str(row[0])[0:9]


            # 记录表后进行重写
            if id not in id_list:
                try:
                    out = open("D:/nudt_work/has/1.csv", "a+", encoding="utf-8")
                    out.writelines(id + '\n')
                    file = urllib.request.urlopen(url)
                    if file.getcode() == 200:
                        ava_list.append(url)
                        print("200:" + url)
                        try:
                            out = open("D:/nudt_work/200url/" + str(i), "a", encoding="utf-8")
                            # 写入到记录表
                            out.writelines(id + url + '\n')
                            news = Article(url)
                            news.download()
                            news.parse()
                            # 可响应可解析,没有error标识,i就是文件名
                            for j in range(len(ava_list)):
                                out1 = open(textpos + str(i) + "." + id + '.text', 'a', encoding='utf-8')
                                out1.writelines(news.title + '\n' + news.text)
                        except newspaper.article.ArticleException:
                            ava_list_unparse = []
                            ava_list_unparse.append("200:" + str(i) + id + url + '\n')
                            out = open(unparse + str(i), 'a+', encoding='utf-8')
                            out.writelines(ava_list_unparse)

                        # 可响应不可解析,不加响应error标识
                        # out3 = open(unparse + str(i) + "." + id + '.text', 'a', encoding='utf-8')
                        # out3.writelines("响应成功不能解析" + url + '\n')
                # 错误类型1,error1
                except urllib.error.URLError as e:
                    # 记录错误，添加error标识
                    reason_list.append(id + url + '\n')
                    # 创建文件夹
                    out = open("D:/nudt_work/error1/" + str(i), "w", encoding="utf-8")
                    print("error1" + str(e.reason) + url)
                    out.writelines(reason_list)
                    # error1中能够爬取的网站
                    try:
                        news = Article(url)
                        news.download()
                        news.parse()
                        out3 = open(textpos + str(i) + "error1" + id + "." + '.text', 'w', encoding='utf-8')
                        out3.writelines(news.title + '\n' + news.text)
                        out.writelines("CanSpider" + str(id) + url + '\n')
                    # 错误不能解析
                    except newspaper.article.ArticleException:
                        unparse_error1_list = []
                        unparse_error1_list.append("error1:" + str(i) + id + url + '\n')
                        out3 = open(unparse + str(i), 'a+', encoding='utf-8')
                        out3.writelines(unparse_error1_list)

                # 错误二

                except urllib.error.URLError as e:
                    # 记录错误，添加error标识
                    code_list.append(id + url + '\n')
                    # 创建文件夹
                    out = open("D:/nudt_work/error1/" + str(i), "w", encoding="utf-8")
                    print("error2" + str(e.reason) + url)
                    out.writelines(code_list)
                    # error2中能够爬取的网站
                    try:
                        news = Article(url)
                        news.download()
                        news.parse()
                        out4 = open(textpos + str(i) + "error2" + id + "." + '.text', 'w', encoding='utf-8')
                        out4.writelines(news.title + '\n' + news.text)
                        out.writelines("CanSpider" + str(id) + url + '\n')
                    # 错误不能解析
                    except newspaper.article.ArticleException:
                        unparse_error2_list = []
                        unparse_error2_list.append("error2:" + str(i) + id + url + '\n')
                        out4 = open(unparse + str(i), 'a+', encoding='utf-8')
                        out4.writelines(unparse_error2_list)


                # 错误三
                except urllib.error.URLError as e:
                    # 记录错误，添加error标识
                    time_list.append(id + url + '\n')
                    # 创建文件夹
                    out = open("D:/nudt_work/error3/" + str(i), "a+", encoding="utf-8")
                    print("error3" + str(e.reason) + url)
                    out.writelines(time_list)
                    # error3中能够爬取的网站
                    try:
                        news = Article(url)
                        news.download()
                        news.parse()
                        out3 = open(textpos + str(i) + "error3:" + id + "." + '.text', 'w', encoding='utf-8')
                        out3.writelines(news.title + '\n' + news.text)
                        out.writelines("CanSpider" + str(id) + url + '\n')
                    # 错误不能解析
                    except newspaper.article.ArticleException:
                        unparse_error3_list = []
                        unparse_error3_list.append("error3" + str(i) + id + url + '\n')
                        out3 = open(unparse + str(i), 'a+', encoding='utf-8')
                        out3.writelines(unparse_error3_list)


                # 错误四
                except urllib.error.URLError as e:
                    # 记录错误，添加error标识
                    connect_list.append(id + url + '\n')
                    # 创建文件夹
                    out = open("D:/nudt_work/error4/" + str(i), "w", encoding="utf-8")
                    print("error3" + str(e.reason) + url)
                    out.writelines(connect_list)
                    # error3中能够爬取的网站
                    try:
                        news = Article(url)
                        news.download()
                        news.parse()
                        out3 = open(textpos + str(i) + "error4:" + id + "." + '.text', 'w', encoding='utf-8')
                        out3.writelines(news.title + '\n' + news.text)
                        out.writelines("CanSpider" + str(id) + url + '\n')
                    # 错误不能解析
                    except newspaper.article.ArticleException:
                        unparse_error4_list = []
                        unparse_error4_list.append("error3" + str(i) + id + url + '\n')
                        out3 = open(unparse + str(i), 'a+', encoding='utf-8')

                        out3.writelines(unparse_error3_list)

            # out = open("D:/nudt_work/has/1.csv", "a+", encoding="utf-8")


        return ava_list, code_list, reason_list, time_list, connect_list


path = "D:/nudt_work/avaurl"
files = os.listdir(path)
# print(file)

# 用*a来控制进程数组
a = 0
while a < 100:
    threads = []
    for j in files[128 * a:128 * (a + 1)]:
        print(j)
        t = threading.Thread(target=analyze_url, args=(
            "D:/nudt_work/avaurl/" + j, j, "D:/nudt_work/parse/text/", "D:/nudt_work/parse/unparse/",))
        threads.append(t)
    if __name__ == "__main__":
        for thr in threads:
            print(thr)
            t.setDaemon(True)
            thr.start()
        thr.join()
        a += 1
        print("all over %s" % time.ctime)

# 时间一个文件16线程 100分钟，4X24X365X4=14016文件  233600小时/24=9700天=26年
# 断点重爬，时间过久跳过
