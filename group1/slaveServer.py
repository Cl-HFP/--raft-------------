#!/usr/bin/env python
# coding: utf-8
from socket import *
# 在这里控制node的加入和删除
import json
import logging
import os
import signal
import sys
import traceback
from multiprocessing import Process

import threading
sys.path.append("..")    #相对路径或绝对路径
from config import config
from raft_node import Node
from rpc import Rpc

conf={"ip":"localhost","port":9900,"node_ip":"localhost","group_id":1,
      "master_ip":"localhost","master_port":8800,
      "buffer_size":1024,"slave_path":"slave_log/"}

logger = logging.getLogger(__name__)

class Slave(object):
    def __init__(self):
        #self.conf = self.load_conf()
        self.rpc_endpoint = Rpc((conf["ip"], conf["port"]))
        self.childrens = {}
        self.resourcesList = ["s1.txt","s2.txt","s3.txt"]  # 在此slave服务器上拥有的资源
        self.group_id = -1
        self.num_node = 0
        self.leader_id=0
        self.leader_addr=()
    @staticmethod
    def create_node(self, meta: dict):  #创建新结点
        node = Node(meta)
        node.run()
    def handle_create_node(self,data):
        num = data["num"]  # 创建结点的数量
        metas = []
        for i in range(num):  # 创建结点的数量
            metas.append({})
            metas[i]["group_id"] = str(self.group_id)
            metas[i]["id"] = str(self.num_node+i)
            metas[i]["addr"] = (conf["node_ip"], conf["port"] + self.num_node + i + 1)
            metas[i]["peers"] = {}
            metas[i]["slave_addr"] = (conf["ip"],conf["port"])   #与slave保持通信
            #metas[i]["conf"]={"node_path":""}
        for i in range(num):
            for j in range(num):
                if i != j:
                    metas[i]["peers"][str(j)] = (conf["node_ip"], conf["port"] + self.num_node + j + 1)
        self.num_node += num
        for i in range(num):  # 创建子进程去创建结点
            logger.info("create node")
            logger.info(metas[i])
            self.save_node_meta(metas[i])
            p = Process(target=self.create_node, args=({}, metas[i]), daemon=True)
            p.start()
            self.childrens[(metas[i]["group_id"], metas[i]["id"])] = p.pid
    def handle_write(self,data):
        response = {"write_node_addr": self.leader_addr,"value":True}
        if len(self.leader_addr)==0:
            response["value"]=False
        if response["value"]:
            logger.info("write request is convert to ip:%s port:%d" % (self.leader_addr[0], self.leader_addr[1]))
        self.rpc_endpoint.send(response,data["src_addr"])
        if not (data["filename"] in self.resourcesList):  #更新资源信息
            self.resourcesList.append(data["filename"])
    def handle_read(self,data):
        response = {"read_node_addr": self.leader_addr, "value": True}
        if not (data["filename"] in self.resourcesList):
            response["value"]=False
        if len(self.leader_addr) == 0:
            response["value"] = False
        if response["value"]:
            logger.info("read request is convert to ip:%s port:%d" % (self.leader_addr[0], self.leader_addr[1]))
        self.rpc_endpoint.send(response, data["src_addr"])
    def handle_delete(self,data):
        response = {"delete_node_addr": self.leader_addr, "value": True}
        if not (data["filename"] in self.resourcesList):
            response["value"] = False
        if len(self.leader_addr) == 0:
            response["value"] = False
        if response["value"]:
            self.resourcesList.remove(data["filename"])
            logger.info("delete request is convert to ip:%s port:%d" % (self.leader_addr[0], self.leader_addr[1]))
        self.rpc_endpoint.send(response, data["src_addr"])
    def handle_rename(self,data):
        response = {"rename_node_addr": self.leader_addr, "value": True}
        if not (data["filename"] in self.resourcesList):
            response["value"] = False
        if len(self.leader_addr) == 0:
            response["value"] = False
        if response["value"]:
            i=self.resourcesList.index(data["filename"])
            self.resourcesList[i]=data["new_name"]
            logger.info("rename request is convert to ip:%s port:%d" % (self.leader_addr[0], self.leader_addr[1]))
        self.rpc_endpoint.send(response, data["src_addr"])
    def handle_create(self,data):
        response = {"create_node_addr": self.leader_addr, "value": True}
        if len(self.leader_addr) == 0:
            response["value"] = False
        if response["value"]:
            logger.info("create file request is convert to ip:%s port:%d" % (self.leader_addr[0], self.leader_addr[1]))
        self.rpc_endpoint.send(response, data["src_addr"])
        if not (data["filename"] in self.resourcesList):  # 更新资源信息
            self.resourcesList.append(data["filename"])
    def stop_node(self, meta: dict):
        pid = self.childrens.pop((meta["group_id"], meta["id"]), None)
        logger.info(pid)
        if not pid:
            return
        os.kill(pid, signal.SIGTERM)

    def save_node_meta(self, meta: dict):
        """save node meta data
        Args:
            meta (dict): meta data
        """
        self.path = conf["slave_path"]
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        filename = os.path.join(self.path, meta["group_id"] + "_" + meta["id"] + ".json")
        with open(filename, "w") as f:
            json.dump(meta, f, indent=4)

    def stop_slave(self):
        #self.rpc_endpoint.close()
        sys.exit(0)

    def listen(self):
        while True:
            try:
                try:
                    data, addr = self.rpc_endpoint.recv()
                    print(self.group_id)
                except Exception as e:
                    data, addr = {}, None
                if data["type"]=="inform_leader":
                    self.leader_id=data["leader_id"]
                    self.leader_addr=data["leader_addr"]
                    leader_info={"type":"leader_info","id":data["leader_id"],"addr":data["leader_addr"],"slave_id":self.group_id}
                    leader_info=json.dumps(leader_info).encode()
                    self.slave.send(leader_info)
                    logger.info("leader changed,new leader:%s"% self.leader_id)
                if data["type"]=="write":
                    logger.info("write %s request from ip:%s port:%d"%(data["filename"],data["src_addr"][0],data["src_addr"][1]))
                    self.handle_write(data)
                if data["type"]=="read":
                    logger.info("read %s request from ip:%s port:%d" % (data["filename"], data["src_addr"][0], data["src_addr"][1]))
                    self.handle_read(data)
                if data["type"]=="delete":
                    logger.info("delete %s request from ip:%s port:%d" % (data["filename"], data["src_addr"][0], data["src_addr"][1]))
                    self.handle_delete(data)
                if data["type"]=="rename":
                    logger.info("rename %s request from ip:%s port:%d" % (
                    data["filename"], data["src_addr"][0], data["src_addr"][1]))
                    self.handle_rename(data)
                if data["type"]=="create_file":
                    logger.info("create %s request from ip:%s port:%d" % (
                    data["filename"], data["src_addr"][0], data["src_addr"][1]))
                    self.handle_create(data)
            except KeyboardInterrupt:
                self.rpc_endpoint.close()
                sys.exit(0)
            except Exception:
                traceback.print_exc()
                logger.info(traceback.format_exc())
    def run(self):
        logger.info("slave begin running...")
        #创建后先尝试与主服务器取得联系
        self.slave = socket(AF_INET, SOCK_STREAM)  # 创建TCP套接字
        self.slave.connect((conf["master_ip"], conf["master_port"]))  # 发起三次握手
        data2 = {"type": "new_slave", "num_node": self.num_node,"resourcesList":self.resourcesList,"id":conf["group_id"],
                 "slave_addr":(conf["ip"],conf["port"])}
        data2 = json.dumps(data2).encode()
        self.slave.send(data2)
        maxsize = conf["buffer_size"]
        data1 =self.slave.recv(maxsize)
        data1=json.loads(data1)
        self.group_id=data1["group_id"]  #从主服务器那里获得组id

        manage_node = threading.Thread(target=self.listen, args=())
        manage_node.start()
        manage_node.join(1.0)

        while True:
            meta = {"group_id": str(self.group_id),
                    "id": "0",
                    "addr": ("localhost", 8900),
                    "peers": {}}
            try:
                data= self.slave.recv(maxsize)
                data=json.loads(data)
                if data["type"] == "create_node":
                    logger.info("create nodes,group_id:%d nodes:%d"%(self.group_id,data["num"]))
                    self.handle_create_node(data)

                elif data["type"] == "stop_node":
                    logger.info("stop node")
                    meta = data["meta"]
                    self.stop_node(meta)

                elif data["type"] == "stop_slave":
                    logger.info("stop slave")
                    self.stop_slave()
            except KeyboardInterrupt:
                self.slave.close()
                sys.exit(0)
            except Exception:
                logger.info(traceback.format_exc())
def main() -> int:
    slave = Slave()
    slave.run()
    return 0
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(funcName)s [line:%(lineno)d] %(message)s",
    )
    sys.exit(main())
