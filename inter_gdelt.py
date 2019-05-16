#!/usr/bin/env python 
# -*- coding:utf-8 -*-

#在文件中提取出url

#!/usr/bin/env python
# -*- coding:utf-8 -*-


#批量分割url地址



import csv
import math
import pandas as pd
import numpy as np
import os
import time
    #gfile为初始文件位置，n_file提取url位置


def get_url(input_file,ouput_file):
        url_list_id=[]
        with open(input_file,encoding='utf-8') as f:
            reader = csv.reader(f)
            for row in reader:
                id = str(row[0])[0:9]
                last_element = list(row)[-1]
                url = last_element.split("	")[-1]
                print(url)
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
        s=map(lambda x:str(x[0])+x[1], dataset)
        #wewant就是我们所需的数据
        wewant=list(s)
        return wewant

