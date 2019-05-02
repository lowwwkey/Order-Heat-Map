#!/usr/bin/env python
# -*-coding:utf-8-*-

'''
往钉钉发送报警消息
'''

import json
import requests

def send_msg(url, text):
    header = {
        "Content-Type": "application/json",
        "Charset": "UTF-8"
        }

    my_data = {"msgtype": "text", 
               "text": {
                         "content":""
                       }
              }
    my_data["text"]["content"] = text

    sendData = json.dumps(my_data)
    sendDatas = sendData.encode("utf-8")
    request = requests.post(url=url, data=sendDatas, headers=header)

if __name__ == '__main__':
    my_url = "https://oapi.dingtalk.com/robot/send?access_token=d74f5035eb7696c5d35f3d432dd43abdca0f1fbb85159cf31ec75a3331314"
    my_text = "你好"
    send_msg(my_url, my_text)
