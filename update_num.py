#!/usr/bin/env python
# -*-coding:utf-8-*-

'''
@File       : update_num.py
@Discription: 更新实时各个商圈、各个车型每十五分钟下单量及无司机单量
@Author     : Guangkai Li
@Date:      : 2019/01/11
'''

import json
import pandas as pd
import sys
import warnings
from collections import defaultdict
from dateutil.parser import parse

warnings.filterwarnings("ignore")

def load_data(file_name):
    city = map(lambda x: str(x), [1,2,3,483,4,342,158,102,37,202,79,414,18,222,319,837,241,413,265,669])
    df = pd.read_csv(file_name, delimiter='\t', error_bad_lines=False)
    return df[df['city_id'].isin(city)]

def get_num(df):
    time_slide = map(lambda x: str(parse(str(int(x)))), list(df.dt))
    car_list = list(map(lambda x: str(int(x)), df.car_type))
    biz_list = list(df.start_biz_id)
    city_list = list(map(lambda x: str(int(x)), df.city_id))
    order_num_list = list(df.order_num)
    nodriver_num_list = list(df.nodriver_nearby_3km)
    
    new_order_num = {}
    new_nodriver_num = {}
    for i in range(len(city_list)):
        new_order_num[city_list[i]] = new_order_num.get(city_list[i], {})
        new_order_num[city_list[i]][biz_list[i]] = new_order_num[city_list[i]].get(biz_list[i], {})
        new_order_num[city_list[i]][biz_list[i]][time_slide[i]] = new_order_num[city_list[i]][biz_list[i]].get(time_slide[i], 0) + order_num_list[i]

        new_nodriver_num[city_list[i]] = new_nodriver_num.get(city_list[i], {})
        new_nodriver_num[city_list[i]][biz_list[i]] = new_nodriver_num[city_list[i]].get(biz_list[i], {})
        new_nodriver_num[city_list[i]][biz_list[i]][time_slide[i]] = new_nodriver_num[city_list[i]][biz_list[i]].get(time_slide[i], 0) + nodriver_num_list[i]

    #--------------------------
    new_order_num_car = {}
    new_nodriver_num_car = {}
    for i in range(len(city_list)):
        new_order_num_car[car_list[i]] = new_order_num_car.get(car_list[i], {})
        new_order_num_car[car_list[i]][city_list[i]] = new_order_num_car[car_list[i]].get(city_list[i], {})
        new_order_num_car[car_list[i]][city_list[i]][biz_list[i]] = new_order_num_car[car_list[i]][city_list[i]].get(biz_list[i], {})
        new_order_num_car[car_list[i]][city_list[i]][biz_list[i]][time_slide[i]] = new_order_num_car[car_list[i]][city_list[i]][biz_list[i]].get(time_slide[i], 0) + order_num_list[i]

        new_nodriver_num_car[car_list[i]] = new_nodriver_num_car.get(car_list[i], {})
        new_nodriver_num_car[car_list[i]][city_list[i]] = new_nodriver_num_car[car_list[i]].get(city_list[i], {})
        new_nodriver_num_car[car_list[i]][city_list[i]][biz_list[i]] = new_nodriver_num_car[car_list[i]][city_list[i]].get(biz_list[i], {})
        new_nodriver_num_car[car_list[i]][city_list[i]][biz_list[i]][time_slide[i]] = new_nodriver_num_car[car_list[i]][city_list[i]][biz_list[i]].get(time_slide[i], 0) + nodriver_num_list[i]
    #-------------------------

    return new_order_num, new_nodriver_num, new_order_num_car, new_nodriver_num_car

def load_json():
    with open("/opt/work/ligk/heatmap/data/order_num_all_beta.json", "r") as load_f:
        order_num = json.load(load_f)
    with open("/opt/work/ligk/heatmap/data/nodriver_num_all_beta.json", "r") as load_ff:
        nodriver_num = json.load(load_ff)
    with open("/opt/work/ligk/heatmap/data/order_num_car_beta.json", "r") as load_f_car:
        order_num_car = json.load(load_f_car)
    with open("/opt/work/ligk/heatmap/data/nodriver_num_car_beta.json", "r") as load_ff_car:
        nodriver_num_car = json.load(load_ff_car)
    return order_num, nodriver_num, order_num_car, nodriver_num_car

def run(file_name):
    df = load_data(file_name)
    order_num, nodriver_num, order_num_car, nodriver_num_car = load_json()
    new_order_num, new_nodriver_num, new_order_num_car, new_nodriver_num_car = get_num(df)

    for city in order_num:
        if city in new_order_num:
            for biz in order_num[city]:
                if biz in new_order_num[city]:
                    order_num[city][biz].update(new_order_num[city][biz])
    for city in new_order_num:
        for biz in new_order_num[city]:
            if biz not in order_num[city]:
                order_num[city][biz] = new_order_num[city].get(biz)

    for city in nodriver_num:
        if city in new_nodriver_num:
            for biz in nodriver_num[city]:
                if biz in new_nodriver_num[city]:
                    nodriver_num[city][biz].update(new_nodriver_num[city][biz])
    for city in new_nodriver_num:
        for biz in new_nodriver_num[city]:
            if biz not in nodriver_num[city]:
                nodriver_num[city][biz] = new_nodriver_num[city].get(biz)

    #-------------------
    for car in order_num_car:
        if car in new_order_num_car:
            for city in order_num_car[car]:
                if city in new_order_num_car[car]:
                    for biz in order_num_car[car][city]:
                        if biz in new_order_num_car[car][city]:
                            order_num_car[car][city][biz].update(new_order_num_car[car][city][biz])
    for car in new_order_num_car:
        for city in new_order_num_car[car]:
            for biz in new_order_num_car[car][city]:
                if car in order_num_car:
                    if biz not in order_num_car[car][city]:
                        order_num_car[car][city][biz] = new_order_num_car[car][city].get(biz)
    
    for car in nodriver_num_car:
        if car in new_nodriver_num_car:
            for city in nodriver_num_car[car]:
                if city in new_nodriver_num_car[car]:
                    for biz in nodriver_num_car[car][city]:
                        if biz in new_nodriver_num_car[car][city]:
                            nodriver_num_car[car][city][biz].update(new_nodriver_num_car[car][city][biz])
    for car in new_nodriver_num_car:
        for city in new_nodriver_num_car[car]:
            for biz in new_nodriver_num_car[car][city]:
                if car in nodriver_num_car:
                    if biz not in nodriver_num_car[car][city]:
                        nodriver_num_car[car][city][biz] = new_nodriver_num_car[car][city].get(biz)
    #-------------------

    with open('/opt/work/ligk/heatmap/data/order_num_all_beta.json', 'w') as order_file:
        json.dump(order_num, order_file, ensure_ascii=False)
    with open('/opt/work/ligk/heatmap/data/nodriver_num_all_beta.json', 'w') as nodriver_file:
        json.dump(nodriver_num, nodriver_file, ensure_ascii=False)
    with open('/opt/work/ligk/heatmap/data/order_num_car_beta.json', 'w') as order_file_car:
        json.dump(order_num_car, order_file_car, ensure_ascii=False)
    with open('/opt/work/ligk/heatmap/data/nodriver_num_car_beta.json', 'w') as nodriver_file_car:
        json.dump(nodriver_num_car, nodriver_file_car, ensure_ascii=False)

    print "更新完毕！"

if __name__ == '__main__':
    _, file_name = sys.argv
    run(file_name)
