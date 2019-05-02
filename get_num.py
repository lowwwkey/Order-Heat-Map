#!/usr/bin/env python
# -*-coding:utf-8-*-

'''
@File       : get_num.py
@Discription: 获取历史一个月各个商圈、各个车型每十五分钟下单量及无司机单量
@Author     : Guangkai Li
@Date:      : 2019/01/10
'''

import json
import pandas as pd
import time
import warnings
from collections import defaultdict
from datetime import timedelta

warnings.filterwarnings("ignore")

def load_data(filename, city):
    """
    Args:
        city:城市id， 列表形式
    """
    df = pd.read_csv(filename, delimiter="\t")
    return df[(df['city_id'].isin(city))&(df['start_biz_id'].notnull())&(df['driver_num_3km'].notnull())]

def save_slice_num(df, time_slide=15, time_window=15):
    """
    获得每个商圈时间切片内订单量，
    储存数据格式为｛‘city_id’:{'biz_id':{'time_slice':num}}｝
    Args:
        df:数据
        time_slide:时间滑动，15min
        time_window:时间窗口，120min
    """
    time_slice = list(map(lambda x: x.replace(minute=x.minute/time_slide*time_slide, second=0), pd.to_datetime(df.create_time)))
    car_list = list(map(lambda x: str(int(x)), df.car_type))
    city_list = list(map(lambda x: str(int(x)), df.city_id))
    biz_list = list(df.start_biz_id)
    driver_num_list = list(df.driver_num_3km)

    order_num = {}
    nodriver_num = {}
    n = time_window/time_slide

    #-------------------
    order_num_car = {}
    nodriver_num_car = {}
    #-------------------

    for i in range(len(time_slice)):
        # ---统计商圈下单量--- 
        order_num[city_list[i]] = order_num.get(city_list[i], defaultdict(dict))
        for j in range(n):
            order_num[city_list[i]][biz_list[i]][str(time_slice[i]-timedelta(minutes=time_slide*j))] = order_num[city_list[i]][biz_list[i]].get(str(time_slice[i]-timedelta(minutes=time_slide*j)), 0) + 1
        # ---统计商圈周围3km无司机订单量--- 
        nodriver_num[city_list[i]] = nodriver_num.get(city_list[i], defaultdict(dict))
        if driver_num_list[i] == 0:
            for j in range(n):
                nodriver_num[city_list[i]][biz_list[i]][str(time_slice[i]-timedelta(minutes=time_slide*j))] = nodriver_num[city_list[i]][biz_list[i]].get(str(time_slice[i]-timedelta(minutes=time_slide*j)), 0) + 1
        
        #-------------------
        order_num_car[car_list[i]] = order_num_car.get(car_list[i], {})
        order_num_car[car_list[i]][city_list[i]] = order_num_car[car_list[i]].get(city_list[i], defaultdict(dict))
        for j in range(n):
            order_num_car[car_list[i]][city_list[i]][biz_list[i]][str(time_slice[i]-timedelta(minutes=time_slide*j))] = order_num_car[car_list[i]][city_list[i]][biz_list[i]].get(str(time_slice[i]-timedelta(minutes=time_slide*j)), 0) + 1
        
        nodriver_num_car[car_list[i]] = nodriver_num_car.get(car_list[i], {})
        nodriver_num_car[car_list[i]][city_list[i]] = nodriver_num_car[car_list[i]].get(city_list[i], defaultdict(dict))
        if driver_num_list[i] == 0:
            for j in range(n):
                nodriver_num_car[car_list[i]][city_list[i]][biz_list[i]][str(time_slice[i]-timedelta(minutes=time_slide*j))] = nodriver_num_car[car_list[i]][city_list[i]][biz_list[i]].get(str(time_slice[i]-timedelta(minutes=time_slide*j)), 0) + 1
        #-------------------

    return order_num, nodriver_num, order_num_car, nodriver_num_car

def main():
    city = [1,2,3,483,4,342,158,102,37,202,79,414,18,222,319,837,241,413,265,669]
    s1 = time.time()
    print "开始导入数据..."
    df = load_data("./data/biz_data_0227.csv", city)
    s2 = time.time()
    print "初始数据导入完毕，用时 %s 秒" % (s2-s1)
    print "开始统计商圈订单/无司机数量..."
    order_num, nodriver_num, order_num_car, nodriver_num_car = save_slice_num(df)
    s3 = time.time()
    print "统计完毕，用时 %s 秒" % (s3-s2)
    print "开始写入数据..."
    with open('./json/order_num_all_beta.json', 'w') as order_file:
        json.dump(order_num, order_file, ensure_ascii=False)
    with open('./json/nodriver_num_all_beta.json', 'w') as nodriver_file:
        json.dump(nodriver_num, nodriver_file, ensure_ascii=False)
    #-------------------------
    with open('./json/order_num_car_beta.json', 'w') as order_file:
        json.dump(order_num_car, order_file, ensure_ascii=False)
    with open('./json/nodriver_num_car_beta.json', 'w') as nodriver_file:
        json.dump(nodriver_num_car, nodriver_file, ensure_ascii=False) 
    #-------------------------
    s4 = time.time()
    print "写入完毕，用时 %s 秒" % (s4-s3)

if __name__ == '__main__':
    main()


