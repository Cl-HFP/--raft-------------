#!/usr/bin/env python
# coding: utf-8
import json
import socket

class Rpc(object):
    def __init__(self, addr: tuple[str, int]=None, timeout: int=None):   #传入ip和端口，以及timeout
        self.ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)#internet家族地址，UDP协议
        if addr:
            self.bind(tuple(addr))#绑定ip和端口
        if timeout:
            self.ss.settimeout(timeout)  #设置超时相应时间
    def bind(self, addr: tuple[str, int]):#端口绑定
        print(addr)
        self.addr = tuple(addr)
        self.ss.bind(addr)
    def settimeout(self, timeout: int):#设置超时时间
        self.ss.settimeout(timeout)

    def send(self, data: dict, addr: tuple[str, int]=None):#向指定IP和端口发送数据
        if addr:
            data = json.dumps(data).encode() #将结构数据转为字符流数据类进行传送
            self.ss.sendto(data, tuple(addr))
    def recv(self, addr: tuple[str, int]=None, timeout: int=None)-> tuple[dict, tuple[str, int]]:
        if addr:
            self.bind(addr)
        if not self.addr:
            raise ("please bind to an addr")
        if timeout:
            self.settimeout(timeout)
        data, addr = self.ss.recvfrom(65535)
        return json.loads(data), addr
    #从端口接收收据，若套接字还未建立，可附加地址参数在此函数创建UDP套接字
    def close(self):
        self.ss.close()#关闭连接
