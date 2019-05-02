#!/usr/bin/env python
# -*-coding:utf-8-*-

'''
@File       : main.py
@Discription: 分车型运力图策略及实现
@Author     : Guangkai Li
@Date:      : 2019/01/15
'''

import json
import logging
import multiprocessing
import numpy as np
import os
import pymysql
import scipy.stats as st
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta
from interval import Interval
from math import exp
from sendmsg import send_msg

logging.basicConfig(level=logging.DEBUG,  
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',  
                    datefmt='%a, %d %b %Y %H:%M:%S',  
                    filename='/opt/work/ligk/log/heat_value_car.log',  
                    filemode='a')  
  
logging.debug('debug message')  
logging.info('info message')  
logging.warning('warning message')  
logging.error('error message')  
logging.critical('critical message') 

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


# ------------第一版-------------
def get_history_num(num_list, pre_time):
    strip_time = datetime.strptime(pre_time, '%Y-%m-%d %H:%M:%S')
    history_time = [[str(strip_time-timedelta(weeks=(i+1), minutes=15*j)) for j in range(8)] for i in range(5)]
    b_history_time = [[str(strip_time-timedelta(weeks=(i+1), minutes=15*4) + timedelta(minutes=15*j)) for j in range(2)] for i in range(5)]
    history_num = [[num_list[j] if j in num_list else 0 for j in i] for i in history_time]
    b_history_num = [[num_list[j] if j in num_list else 0 for j in i] for i in b_history_time]
    b_now_time = [str(strip_time-timedelta(minutes=15*4) + timedelta(minutes=15*j)) for j in range(2)]
    b_now_num = [num_list[i] if i in num_list else 0 for i in b_now_time]
    return np.array(history_num).sum(axis=1), np.array(b_history_num).sum(axis=1), np.array(b_now_num).sum()


# ------------第二版-------------
def get_history_num_beta(num_list, pre_time):
    strip_time = datetime.strptime(pre_time, '%Y-%m-%d %H:%M:%S')
    history_time = [[str(strip_time-timedelta(days=(i+1))+timedelta(minutes=15*j)) for j in range(8)] for i in range(10)]
    b_history_time = [[str(strip_time-timedelta(days=(i+1), minutes=15*3) - timedelta(minutes=15*j)) for j in range(4)] for i in range(10)]
    history_num = [[num_list[j] if j in num_list else 0 for j in i] for i in history_time]
    b_history_num = [[num_list[j] if j in num_list else 0 for j in i] for i in b_history_time]
    b_now_time = [str(strip_time-timedelta(minutes=15*3) - timedelta(minutes=15*j)) for j in range(4)]
    b_now_num = [num_list[i] if i in num_list else 0 for i in b_now_time]
    return np.array(history_num).sum(axis=1), np.array(b_history_num).sum(axis=1), np.array(b_now_num).sum()

    
def history_plus_now(num_list, pre_time):
    history_num, b_history_num, b_now_num = get_history_num_beta(num_list, pre_time)
    b_history_mean = b_history_num.mean()
    history_mean = history_num.mean()
    '''
    # 一期策略
    if b_history_mean != 0:
        pre_num = float(b_now_num)/b_history_num.mean()*history_num.mean()
    else:
        pre_num = history_mean
    if np.isinf(pre_num) or np.isnan(pre_num):
        return 0
    else:
        return round(pre_num)
    '''

    if len(set(b_history_num)) != 1:
        b_his_confidence = st.t.interval(0.9, len(b_history_num)-1, loc=np.mean(b_history_num), scale=st.sem(b_history_num))
        if b_now_num in Interval(b_his_confidence[0], b_his_confidence[1]):
            return float(history_mean)
        else:
            pre_num = history_mean + 1*float(b_now_num-b_history_mean)/b_history_mean*(1.0-exp(-0.0767*b_history_mean))*history_mean + 0.0*float(b_now_num-b_history_mean) 
    elif float(b_history_mean) == 0.0:
        return float(history_mean)
    else: 
        pre_num = history_mean + 1*float(b_now_num-b_history_mean)/b_history_mean*(1.0-exp(-0.0767*b_history_mean))*history_mean + 0.0*float(b_now_num-b_history_mean)
    if np.isinf(pre_num) or np.isnan(pre_num):
        return 0
    else:
        return float(pre_num)

def get_heat_value(city, pre_time, return_dict, order_num, nodriver_num):
    biz_heat_value = {}
    co = [0.6, 0.8]
    strip_time = datetime.strptime(pre_time, '%Y-%m-%d %H:%M:%S')
    hour = strip_time.hour
    for biz in order_num[city]:
        order_num_list = order_num[city][biz]
        pre_num_order = history_plus_now(order_num_list, pre_time)
        if city not in nodriver_num or biz not in nodriver_num[city]:
            pre_num_nodriver = 0
        else:
            nodriver_num_list = nodriver_num[city][biz]
            pre_num_nodriver = history_plus_now(nodriver_num_list, pre_time)
        if hour in Interval(10, 19, lower_closed=True):
            heat_value = co[0]*pre_num_order + co[1]*pre_num_nodriver
        else:
            heat_value = co[1]*pre_num_order + co[0]*pre_num_nodriver     
        biz_heat_value[biz] = round(heat_value)
    return_dict[city] = biz_heat_value
    print "process of {0} completed!".format(city)            
    logging.info("process of {0} completed!".format(city))

'''
def get_heat_value_car(car, pre_time. return_dict):
    biz_heat_value = {}
    co = [0.6, 0.8]
    strip_time = datatime.striptime(pre_time, '%Y-%m-%d %H:%M:%S')
    hour = strip_time.hour
    for car in order_num_car
'''

def global_value():
    global order_num, nodriver_num, order_num_car, nodriver_num_car
    order_num, nodriver_num, order_num_car, nodriver_num_car = load_json()

def run_car(pre_time):
    start_time = time.time()
    logging.info("开始运行程序...")

    global_value()

    car_heat_value = {}
    for car in order_num_car:
        print car
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        
        city = order_num_car[car].keys()
        jobs = []
        
        for i in range(len(city)):
            p = multiprocessing.Process(target=get_heat_value, args=(city[i], pre_time, return_dict, order_num_car[car], nodriver_num_car[car]))
            jobs.append(p)
            p.start()

        for proc in jobs:
            proc.join()

        car_heat_value[car] = dict(return_dict)
    return car_heat_value

def run(pre_time):
    start_time = time.time()
    logging.info("开始运行程序...")
    manager = multiprocessing.Manager()
    return_dict = manager.dict()

    global_value()
    city = map(lambda x: str(x), [1,2,3,483,4,342,158,102,37,202,79,414,18,222,319,837,241,413,265,669])
    jobs = []
    # pre_time = '2018-12-05 12:45:00'
    logging.info("预测时间：%s" % pre_time)

    for i in range(len(city)):
        p = multiprocessing.Process(target=get_heat_value, args=(city[i], pre_time, return_dict, order_num, nodriver_num))
        jobs.append(p)
        p.start()

    for proc in jobs:
        proc.join()

    end_time = time.time()
    logging.info("预测结束，用时：%s" % (end_time-start_time))
    # with open("../json/heat_value_"+pre_time+".json", "w") as f:
        # json.dump(dict(return_dict), f, ensure_ascii=False)
    
    return dict(return_dict)    

def write_mysql(heat_value, car_heat_value, pre_time):
    city_name_dict = {'1':'北京', '2':'上海', '3':'广州', '483':'西安', '4':'深圳', '342':'郑州', '158':'武汉', '102':'成都', '37':'重庆', '202':'哈尔滨', '79':'杭州', '414':'长沙', '18':'天津', '222':'佛山', '319':'长春', '837':'合肥', '241':'石家庄', '413':'东莞', '265':'济南', '669':'南昌'}
    
    city_code_dict = {'北京':'010', '上海':'021', '广州':'020', '西安':'029', '深圳':'0755', '郑州':'0371', '武汉':'027', '成都':'028', '重庆':'023', '哈尔滨':'0451', '杭州':'0571', '长沙':'0731', '天津':'022', '佛山':'0757', '长春':'0431', '合肥':'0551', '石家庄':'0311', '东莞':'0769', '济南':'0531', '南昌':'0791'}
    
    my_url = "" 
    pre_day = pre_time.split(' ')[0]
    if os.path.isfile("/opt/work/ligk/heatmap/data/point_location_%s.json"%pre_day):
        with open("/opt/work/ligk/heatmap/data/version.txt", "w") as v_f:
            v_f.write(pre_day)
        with open("/opt/work/ligk/heatmap/data/point_location_"+pre_day+".json", "r") as f:
            point_location = json.load(f)
    else:
        with open("/opt/work/ligk/heatmap/data/version.txt", "r") as v_f:
            day = v_f.readline().strip()
        with open("/opt/work/ligk/heatmap/data/point_location_"+day+".json", "r") as f:
            point_location = json.load(f)
        pre_time = day+' '+pre_time.split(' ')[1]    
    
    values = []

    #---------write into mysql---------
    send_msg(my_url, "本次写入预测%s运力图数据" % pre_time)
    print "begin write mysql..."
    try:
        conn = pymysql.connect(host='', port=, user='', password='', db='')
    except Exception, e:
        print e
        send_msg(my_url, "连接数据库失败！错误信息：%s" % e)
        sys.exit()

    cursor = conn.cursor()

    cursor.execute("select ifnull(max(capacity_version), 0) from sy_app_capacity_hot_map_batch")
    result = cursor.fetchone()
    
    capacity_version = result[0] + 1
    pts = []
    pts_chongqing = []
    for city in point_location:
        max_val = max(heat_value[city].values())
        min_val = min(heat_value[city].values())
        k = 9.0/(max_val-min_val+0.1)
        for biz in point_location[city][pre_time]:
            if biz in heat_value[city]:
                val = heat_value[city][biz]
                if float(val) > 0:
                    val = 1.0+k*(heat_value[city][biz]-min_val)
                    city_code = city_code_dict[city_name_dict[city]]
                    city_name = city_name_dict[city]
                    lng = point_location[city][pre_time][biz][0]
                    lat = point_location[city][pre_time][biz][1]
                    radius = 50
                    version = capacity_version
                    create_time = pre_time
                    value = (city_code, city_name, '-1', lng, lat, val, radius, version, create_time)
                    values.append(value)
                    
                    if int(city) == 79:
                        pt = [lat, lng, val]
                        pts.append(pt)
                    if int(city) == 37:
                        pts_chongqing.append([lat, lng, val])
    
    send_msg(my_url, "杭州市数据为：%s, 重庆市数据为: %s!" % (len(pts), len(pts_chongqing)))

    for car in car_heat_value:
        for city in car_heat_value[car]:
            max_val = max(car_heat_value[car][city].values())
            min_val = min(car_heat_value[car][city].values())
            k = 9.0/(max_val-min_val+0.1)
            for biz in point_location[city][pre_time]:
                if biz in car_heat_value[car][city]:
                    val = car_heat_value[car][city][biz]
                    if float(val) > 0:
                        val = 1.0+k*(car_heat_value[car][city][biz]-min_val)
                        city_code = city_code_dict[city_name_dict[city]]
                        city_name = city_name_dict[city]
                        lng = point_location[city][pre_time][biz][0]
                        lat = point_location[city][pre_time][biz][1]
                        radius = 50
                        version = capacity_version
                        create_time = pre_time
                        value = (city_code, city_name, car, lng, lat, val, radius, version, create_time)
                        values.append(value)
                        if str(city_code) == '0571' and str(car) == '3':
                            values.append((city_code, city_name, '1', lng, lat, val, radius, version, create_time))
    hz_num = defaultdict(dict)
    for car in car_heat_value:
        for city in car_heat_value[car]:
            if int(city) == 79:
                hz_num['%s'%city][car] = len({k:v for k,v in car_heat_value[car][city].items() if v>0})
            if int(city) == 37:
                hz_num['%s'%city][car] = len({k:v for k,v in car_heat_value[car][city].items() if v>0})
    send_msg(my_url, '杭州:%s' % hz_num['79'])
    send_msg(my_url, '重庆:%s' % hz_num['37'])

    sql = "insert into sy_app_capacity_hot_map(city_code, city_name, car_type, lng, lat, val, radius, version, pre_time) values(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    
    if len(values) == 0:
        send_msg(my_url, "数据为0，程序终止！")
        sys.exit()

    try:
        conn.ping(reconnect=True)
        cursor.executemany(sql, values)
        conn.commit()
    except Exception, e:
        conn.rollback()
        print e
        send_msg(my_url, "写库异常，程序终止！错误：%s" % e)
        sys.exit()
    try:
        conn.ping(reconnect=True)
        cursor.execute("insert into sy_app_capacity_hot_map_batch(capacity_version) values(%s)" % capacity_version)
        conn.commit()
    except Exception, e:
        conn.rollback()
        print e
    finally:
        cursor.close()
        conn.close()
    send_msg(my_url, "写库成功，共写入%s条数据" % len(values))
    print "complete write mysql! a total of %s record!" % len(values)
        
if __name__ == '__main__':
    _, pre_time = sys.argv
    heat_value = run(pre_time)
    print heat_value.keys()
    car_heat_value = run_car(pre_time)
    print car_heat_value.keys()
    # with open('/opt/test/liguangkai/heatmap/json/car_heat_value.json', 'w') as f:
        # json.dump(car_heat_value, f, ensure_ascii=False)
    write_mysql(heat_value, car_heat_value, pre_time)
    
