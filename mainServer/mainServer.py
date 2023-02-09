#!/usr/bin/env python
# coding: utf-8
# 控制面中心节点, 进行group创建和管理
from socket import *
import json
import logging
import os
import sys
import traceback
import threading
import time
#from group import Group
sys.path.append("..")    #相对路径或绝对路径
from rpc import Rpc #外部代码
from RWLock import RWLock
logger = logging.getLogger(__name__)
#logger.propagate = False  # 避免重复打印
conf={"ip":"localhost","port":8600,"listen_port":8800,
      "buffer_size":1024}
slaves={}
class Master(object):
    def __init__(self):
        #self.conf = self.load_conf()
        #self.rpc_endpoint = Rpc(conf["ip"], conf["listen_port"])
        #维护组数，即slave个数，一个slave管理一个组
        self.num_group=0   #组数
        self.groups = []   #group_id数
        self.num_node_groups={}  #组id对应的结点数
        self.resources_groups={}  #组id对应的资源
        self.last_update={}  #文件名对应的上一次更新时间
        self.slave_addr={}   #slave的IP和端口
        #self.slaves={}    #组id对应的slave的socket
        self.clients=[]
        self.server_stats = {}
        self.group_stats = {}
        self.port_used = 10000
        self.rpc_endpoint = Rpc((conf["ip"], conf["port"]))
        self.mutexs={}
        file_handler = logging.FileHandler(filename="test.log")
        file_handler.setLevel(logging.INFO)
        logger.addHandler(file_handler)
    def read_handle(self,data,client): #读请求处理
        if self.mutexs.get(data["filename"],-1)==-1:
            self.mutexs[data["filename"]]=RWLock()
        self.mutexs[data["filename"]].read_acquire()
        temp=-1
        for k in self.resources_groups:  #先查找是否有这个文件
            for j in self.resources_groups[k]:
                if j==data["filename"]:
                    temp=k
                    break
        response = {"type": "read_response", "value": True}
        if temp==-1:
            response["value"]=False  #没有这个文件
        else:
            response["read_addr"] = self.slave_addr[temp]
        response = json.dumps(response).encode()
        client.send(response)
        logger.info("read request is convert to ip:%s port:%d" % (self.slave_addr[temp][0], self.slave_addr[temp][1]))
        self.mutexs[data["filename"]].read_release()
    def last_update_handle(self,data,client):  #查找文件上次更新时间
        response={"type":"last_update_response","value":True}
        if data["filename"] in self.last_update:
            response["time"]=self.last_update[data["filename"]]
        else:
            response["value"]=False
        response=json.dumps(response).encode()
        client.send(response)
    def list_handle(self,client):
        files=[]
        for i in self.resources_groups:
            files.extend(self.resources_groups[i])
        response = {"type": "list_response", "files":files,"value": True}
        response = json.dumps(response).encode()
        client.send(response)
    def write_handle(self,data,client):
        if self.mutexs.get(data["filename"],-1)==-1:
            self.mutexs[data["filename"]]=RWLock()
        self.mutexs[data["filename"]].write_acquire()
        temp = 0
        Max = 9999999
        tag=0
        for k in self.resources_groups:
            for j in self.resources_groups[k]:
                if j==data["filename"]:
                    tag=1
                    temp=k
                    break
            if tag:
                break
        if tag==0:     #没有这个文件，需要第一次写
            for k in self.resources_groups:  #简单地实现负载均衡
                if len(self.resources_groups[k]) < Max:
                    Max = len(self.resources_groups[k])
                    temp = k
        response={"type":"write_response","value":True}
        if(len(self.resources_groups)==0):  #如果目前一个集群都没有，则不能写
            response["value"]=False
        else:    #有集群，可以写
            self.last_update[data["filename"]]=time.gmtime(time.time())  #将当前时间作为数据的最新更新时间
            logger.info("write request is convert to ip:%s port:%d" % (self.slave_addr[temp][0], self.slave_addr[temp][1]))
            if not (data["filename"] in self.resources_groups[temp]):
                self.resources_groups[temp].append(data["filename"])
        response["write_addr"]=self.slave_addr[temp]
        response=json.dumps(response).encode()
        client.send(response)
        self.mutexs[data["filename"]].write_release()
    def delete_handle(self,data,client):
        if self.mutexs.get(data["filename"],-1)==-1:
            self.mutexs[data["filename"]]=RWLock()
        self.mutexs[data["filename"]].write_acquire()
        temp = -1
        for k in self.resources_groups:
            for j in self.resources_groups[k]:
                if j == data["filename"]:
                    temp = k
                    break
        response = {"type": "delete_response", "value": True}
        if temp == -1:
            response["value"] = False  # 没有这个文件
        else:
            response["delete_addr"] = self.slave_addr[temp]
            self.resources_groups[temp].remove(data["filename"])
            logger.info("delete request is convert to ip:%s port:%d" % (self.slave_addr[temp][0], self.slave_addr[temp][1]))
        response = json.dumps(response).encode()
        client.send(response)
        self.mutexs[data["filename"]].write_release()
    def rename_handle(self,data,client):
        if self.mutexs.get(data["filename"],-1)==-1:
            self.mutexs[data["filename"]]=RWLock()
        self.mutexs[data["filename"]].write_acquire()
        temp = -1
        for k in self.resources_groups:
            for j in self.resources_groups[k]:
                if j == data["filename"]:
                    temp = k
                    break
        response = {"type": "rename_response", "value": True}
        if temp == -1:
            response["value"] = False  # 没有这个文件
        else:
            response["rename_addr"] = self.slave_addr[temp]
            self.last_update[data["new_name"]] = time.gmtime(time.time())
            logger.info("rename request is convert to ip:%s port:%d" % (self.slave_addr[temp][0], self.slave_addr[temp][1]))
            i = self.resources_groups[temp].index(data["filename"])  # 修改资源记录
            self.resources_groups[temp][i] = data["new_name"]
        response = json.dumps(response).encode()
        client.send(response)
        self.mutexs[data["filename"]].write_release()
    def create_handle(self,data,client):
        if self.mutexs.get(data["filename"], -1) == -1:
            self.mutexs[data["filename"]] = RWLock()
        self.mutexs[data["filename"]].write_acquire()
        temp = 0
        Max = 9999999
        tag = 0
        for k in self.resources_groups:
            for j in self.resources_groups[k]:
                if j == data["filename"]:
                    tag = 1
                    temp = k
                    break
            if tag:
                break
        response = {"type": "create_response", "value": True}
        if tag == 0:  # 没有这个文件，需要创建
            for k in self.resources_groups:  # 简单地实现负载均衡
                if len(self.resources_groups[k]) < Max:
                    Max = len(self.resources_groups[k])
                    temp = k
        else:
            response["value"] = False #已经有这个文件
        if (len(self.resources_groups) == 0):  # 如果目前一个集群都没有，则不能写
            response["value"] = False
        else:  # 有集群，可以写
            self.last_update[data["filename"]] = time.gmtime(time.time())  # 将当前时间作为数据的最新更新时间
            logger.info(
                "create file request is convert to ip:%s port:%d" % (self.slave_addr[temp][0], self.slave_addr[temp][1]))
            if not (data["filename"] in self.resources_groups[temp]):  #更新资源记录
                self.resources_groups[temp].append(data["filename"])
            response["create_addr"] = self.slave_addr[temp]
        response = json.dumps(response).encode()
        client.send(response)
        self.mutexs[data["filename"]].write_release()
    #告知集群登录状态给客户端的管理员
    def group_info(self,client):
        # response={}
        # response["type"] = "group_info"
        # response["group_id"] = self.groups
        # temp = []
        # for k in self.groups:
        #     temp.append(self.num_node_groups[k])
        # response["group_node"] = temp.copy()
        # response = json.dumps(response).encode()
        # client.send(response)
        response = {"type":"group_info","group_id":"","group_node":""}
        response["type"] = "group_info"
        response["group_id"] = self.groups
        temp = []
        for k in self.groups:
            temp.append(self.num_node_groups[k])
        response["group_node"] = temp.copy()
        self.rpc_endpoint.send(response,client)
    def request_handle(self,client, client_add):  #处理客户端请求
        logger.info("accept connection with %s",client_add)#记录日志
        maxsize=conf["buffer_size"]#读写缓存大小
        while True:
            response = {'type': 'none'}
            try:
                data= client.recv(maxsize)#从TCP端口接收数据报文
                data= json.loads(data)  #解包数据，得到一个字典类型的数据结构
                id_level=0
                if data["type"]=="ID_check":     #用户权限确认,用户进行登录
                    if data["character"]=="user":
                        id_level=1
                    if data["character"]=="manager":
                        id_level=2
                    self.group_info(data["addr2"])
                    self.clients.append(data["addr2"])
                elif data["type"]=="read":    #读请求
                    logger.info("read %s request from ip:%s port:%d" % (data["filename"], data["src_addr"][0], data["src_addr"][1]))
                    self.read_handle(data, client)
                elif data["type"]=="last_update":     #缓存更新机制，请求文件最新更新时间
                    logger.info("latest update of %s request from ip:%s port:%d" % (data["filename"], data["src_addr"][0], data["src_addr"][1]))
                    self.last_update_handle(data, client)
                elif data["type"]=="write":
                    logger.info("write %s request from ip:%s port:%d"%(data["filename"],data["src_addr"][0],data["src_addr"][1]))
                    self.write_handle(data,client)
                elif data["type"]=="delete":
                    logger.info("delete %s request from ip:%s port:%d" % (data["filename"], data["src_addr"][0], data["src_addr"][1]))
                    self.delete_handle(data,client)
                elif data["type"]=="rename":
                    logger.info("rename %s request from ip:%s port:%d" % (data["filename"], data["src_addr"][0], data["src_addr"][1]))
                    self.rename_handle(data, client)
                elif data["type"]=="list":
                    logger.info(" list request from ip:%s port:%d" % (data["src_addr"][0], data["src_addr"][1]))
                    self.list_handle(client)
                elif data["type"]=="create_file":
                    logger.info(" create file request from ip:%s port:%d" % (data["src_addr"][0], data["src_addr"][1]))
                    self.create_handle(data,client)
                elif data["type"]=="new_slave":  #slave连接通知
                    self.num_group+=1
                    response["type"]="response"
                    g_id=data["id"]
                    response["group_id"]=g_id
                    self.num_node_groups[g_id] = data["num_node"]
                    self.groups.append(g_id)
                    self.resources_groups[g_id]=data["resourcesList"].copy()
                    self.slave_addr[g_id]=data["slave_addr"]
                    logger.info("find new slave, group_id:%d nodes:%d" %(g_id,data["num_node"]))
                    slaves[g_id]=client
                    response=json.dumps(response).encode()
                    client.send(response)
                    for i in data["resourcesList"]:
                        self.mutexs[i]=RWLock()
                        self.last_update[i] = time.gmtime(time.time())
                    for c in self.clients:
                        self.group_info(c)
                elif data["type"]=="create_node":  #客户端请求创建节点
                    logger.info("create nodes,group_id:%d num:%d"%(data["group_id"],data["num"]))
                    forward_data={}     #转发给相应集群进行处理
                    forward_data["type"]="create_node"
                    forward_data["num"]=data["num"]
                    forward_data=json.dumps(forward_data).encode()
                    slaves[data["group_id"]].send(forward_data)
                    self.num_node_groups[data["group_id"]]=data["num"]
                    for c in self.clients:
                        self.group_info(c)
                elif data["type"] == "leader_info":  # 客户端请求创建节点
                    logger.info("leader inform,leader_id:%s" % (data["id"]))
                    forward_data = {}  # 转发给相应集群进行处理
                    forward_data["type"] = "leader_info"
                    forward_data["id"] = data["id"]
                    forward_data["group_id"]=data["slave_id"]
                    for c in self.clients:
                        self.rpc_endpoint.send(forward_data,c)

            except KeyboardInterrupt:
                client.close()
                sys.exit(0)
            except Exception:
                logger.info(traceback.format_exc())
                break
        client.close()
    def manage_thread(self):
        data={"type":"create_node","group_id":-1,"num":3}
        while(True):
            #print(1)
            if(self.num_group):
                print(2)
                for i in self.groups:
                    data=json.dumps(data).encode()
                    self.slaves[i].send(data)
                break
            time.sleep(5)
    def run(self):
        logger.info("master begin running...")
        server = socket(AF_INET, SOCK_STREAM)  # 创建TCP套接字
        server.bind((conf["ip"], conf["listen_port"]))  # 监听所有IP
        server.listen(10)  # 最大排队数
        while True:
            # client是新的在服务端的socket，用来传输数据 ，client_add包括客户端的地址
            client, client_add = server.accept()  # 如果没有连接，则进行阻塞，不会往下执行
            thread = threading.Thread(target=self.request_handle, args=(client, client_add))
            thread.start()
    def listen(self,server):
        while True:
            # client是新的在服务端的socket，用来传输数据
            # client_add包括客户端的地址
            client, client_add = server.accept()  # 如果没有连接，则进行阻塞，不会往下执行
            thread = threading.Thread(target=self.request_handle, args=(client, client_add))
            thread.start()

def main() -> int:
    master = Master()
    master.run()
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,  # 设置日记级别
        format="%(asctime)s %(levelname)s %(name)s %(funcName)s [line:%(lineno)d] %(message)s",  # 设置日志格式
    )
    sys.exit(main())
