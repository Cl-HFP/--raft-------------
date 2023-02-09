#!/usr/bin/env python
# coding: utf-8
from socket import *
import pickle
import threading
import datetime
import time
import logging
import os
import random
import  json
import time
import traceback
import sys
import tkinter
sys.path.append("..")    #相对路径或绝对路径
from config import config

from rpc  import Rpc
conf={"ip":"localhost","port":50051,"port2":50061,
      "master_ip":"localhost","master_port":8800,
      "buffer_size":1024}
logger = logging.getLogger(__name__)
class Client(object):
    def __init__(self):
        self.rpc_endpoint = Rpc((conf["ip"], conf["port"]))
        self.rpc_endpoint2 = Rpc((conf["ip"], conf["port2"]))
        self.client = socket(AF_INET, SOCK_STREAM)  # 创建TCP套接字
        self.addr=(conf["ip"], conf["port"])
        self.addr2 = (conf["ip"], conf["port2"])
        self.server_addr=(conf["master_ip"],conf["master_port"])
        self.maxsize=conf["buffer_size"]
        self.id=""
        self.groups=[]     #连接成功的集群
        self.groups_state=["未连接","未连接","未连接","",""]
        self.groups_name=["group1","group2","group3","",""]
        self.groups_num=["","","","",""]
        self.leader=["","","","",""]
        self.status=False #是否打开第二个界面
    #写操作处理
    def write_handle(self,filename):
        if os.path.exists(filename)==False:#先判断本地是否有要写的文件，若无则无法向服务器写，直接返回
            logger.info("file:%s doesn't exist!"%filename)
            return
        data = {"type": "write", "filename": filename,"src_addr":(conf["ip"],conf["port"])}
        data = json.dumps(data).encode()
        self.client.send(data)  # 发送文件
        data = self.client.recv(self.maxsize)
        data = json.loads(data)    #现在知道了要往哪个slave服务器写
        if data["value"]==True:  #主服务器运行请求，向从服务器转发
            data2 = {"type": "write", "filename": filename,"src_addr":(conf["ip"],conf["port"])} #向slave服务器发起写请求
            self.rpc_endpoint.send(data2,data["write_addr"])
            data2,addr=self.rpc_endpoint.recv()
            if data2["value"]==True:  #从服务器运行请求，向leader节点发出操作请求
                logger.info("write file at node: ip:%s port:%d"%(data2["write_node_addr"][0],data2["write_node_addr"][1]))
                data3={"type":"client_append_entries","op":"write","mode":"new","filename":filename,"content":""}
                with open(filename, "r+") as in_file:#打开要写的文件
                    data3["content"] = in_file.read(1024)#读取首个1024字节，以覆盖写模式提交
                    while data3["content"]:
                        self.rpc_endpoint.send(data3,data2["write_node_addr"])#发送data3
                        data3["content"] = in_file.read(1024)#读取剩下字节，以追加写模式发送
                        data3["mode"]="append"
            else:
                logger.info("write request was rejected by slave server!")
        else :
            logger.info("write request was rejected by master server!")
    def read_file(self):
        while True:
            data,addr=self.rpc_endpoint.recv()
            if data["type"]=="read_node_response":
                if data["mode"] == "new":
                    with open(data["filename"], "w+") as out_file:
                        out_file.write(data["content"])
                elif data["mode"] == "append":
                    with open(data["filename"], "a+") as out_file:
                        out_file.write(data["content"])
                else:
                    break
        logger.info("finish read %s" % data["filename"])
    def read_handle(self,filename):
        if os.path.exists(filename):  #如果已经有该文件
            data={"type":"last_update","filename":filename,"src_addr":self.addr}
            data = json.dumps(data).encode()
            self.client.send(data)  # 向主服务器询问文件上次的更新时间
            response=self.client.recv(self.maxsize)
            response=json.loads(response)
            #如果没有更新记录，或者没有更新的修改，则直接读取缓存文件，不需要请求资源
            if (response["value"]==False )or (response["time"]<=time.gmtime(os.path.getmtime(filename))):
                logger.info("%s chache hit!"%filename)
                return
        data = {"type": "read", "filename": filename, "src_addr": (conf["ip"], conf["port"])}
        data = json.dumps(data).encode()
        self.client.send(data)  # 向主服务器发起读请求
        data = self.client.recv(self.maxsize)
        data = json.loads(data)  # 现在知道了要往哪个slave服务器写
        if data["value"] == True:    #如果允许读
            data2 = {"type": "read", "filename": filename, "src_addr": (conf["ip"], conf["port"])}  # 向slave服务器发起写请求
            self.rpc_endpoint.send(data2, data["read_addr"])
            data2, addr = self.rpc_endpoint.recv()
            if data2["value"] == True:
                logger.info(
                    "read file at node: ip:%s port:%d" % (data2["read_node_addr"][0], data2["read_node_addr"][1]))
                data3 = {"type": "client_append_entries", "op": "read", "mode": "", "filename": filename,
                         "src_addr":self.addr}
                self.rpc_endpoint.send(data3, data2["read_node_addr"])
                p=threading.Thread(target=self.read_file,args=())
                p.start()
            else:
                logger.info("read request was rejected by slave server!")
        else:
            logger.info("read request was rejected by master server!")
    def delete_handle(self,filename):
        if os.path.exists(filename)==True:
            os.remove(filename)
        data = {"type": "delete", "filename": filename,"src_addr":(conf["ip"],conf["port"])}
        data = json.dumps(data).encode()
        self.client.send(data)  #向主服务器发送请求
        data = self.client.recv(self.maxsize)
        data = json.loads(data)    #现在知道了要往哪个slave服务器去删除记录
        if data["value"]==True:
            data2 = {"type": "delete", "filename": filename,"src_addr":(conf["ip"],conf["port"])} #向slave服务器发起写请求
            self.rpc_endpoint.send(data2,data["delete_addr"])
            data2,addr=self.rpc_endpoint.recv()
            if data2["value"]==True:
                logger.info("delete file at node: ip:%s port:%d"%(data2["delete_node_addr"][0],data2["delete_node_addr"][1]))
                data3={"type":"client_append_entries", "op":"delete", "mode":"","filename":filename,"content":"","src_addr":self.addr}
                self.rpc_endpoint.send(data3, data2["delete_node_addr"])
            else:
                logger.info("delete request was rejected by slave server!")
        else :
            logger.info("delete request was rejected by master server!")
    def rename_handle(self,filename,new_name):
        if os.path.exists(filename)==True:  #如果缓存中有，则也修改名称
            os.rename(filename,new_name)
        data = {"type": "rename", "filename": filename,"new_name":new_name,"src_addr":(conf["ip"],conf["port"])}
        data = json.dumps(data).encode()
        self.client.send(data)  #向主服务器发送请求
        data = self.client.recv(self.maxsize)
        data = json.loads(data)    #现在知道了要往哪个slave服务器去删除记录
        if data["value"]==True:
            data2 = {"type": "rename", "filename": filename,"new_name":new_name,"src_addr":(conf["ip"],conf["port"])} #向slave服务器发起写请求
            self.rpc_endpoint.send(data2,data["rename_addr"])
            data2,addr=self.rpc_endpoint.recv()
            if data2["value"]==True:
                logger.info("rename file at node: ip:%s port:%d"%(data2["rename_node_addr"][0],data2["rename_node_addr"][1]))
                data3={"type":"client_append_entries", "op":"rename", "mode":"","filename":filename,
                       "new_name":new_name,"content":"","src_addr":self.addr}
                self.rpc_endpoint.send(data3, data2["rename_node_addr"])
            else:
                logger.info("rename request was rejected by slave server!")
        else :
            logger.info("rename request was rejected by master server!")
    def list_handle(self):    #查询所有存在的文件
        data = {"type": "list","src_addr": (conf["ip"], conf["port"])}
        data = json.dumps(data).encode()
        self.client.send(data)  # 向主服务器发送请求

        data = self.client.recv(self.maxsize)
        data = json.loads(data)  # 得到查询结果
        logger.info(data["files"])
        s=""
        for i in data["files"]:
            s+=i+"\n"
        self.text2.set(s)
        return data["files"]   #一个列表
    def create_handle(self,filename):
        data = {"type": "create_file", "filename": filename,  "src_addr": (conf["ip"], conf["port"])}
        data = json.dumps(data).encode()
        self.client.send(data)  # 向主服务器发送请求
        data = self.client.recv(self.maxsize)
        print(2)
        data = json.loads(data)  # 现在知道了要往哪个slave服务器去创建文件
        if data["value"] == True:
            data2 = {"type": "create_file", "filename": filename,
                     "src_addr": (conf["ip"], conf["port"])}  # 向slave服务器发起写请求
            self.rpc_endpoint.send(data2, data["create_addr"])
            data2, addr = self.rpc_endpoint.recv()
            if data2["value"] == True:
                logger.info(
                    "rename file at node: ip:%s port:%d" % (data2["create_node_addr"][0], data2["create_node_addr"][1]))
                data3 = {"type": "client_append_entries", "op": "create", "mode": "", "filename": filename,
                          "content": "", "src_addr": self.addr}
                self.rpc_endpoint.send(data3, data2["create_node_addr"])
            else:
                logger.info("create file request was rejected by slave server!")
        else:
            logger.info("create file request was rejected by master server!")
    #登录界面
    def mainForm(self):
        self.root = tkinter.Tk()
        self.root.title("期末考核项目：分布式文件系统  客户端")
        self.root.geometry("500x400")
        self.root.maxsize(800, 500)
        # 用户登录按钮
        self.button = tkinter.Button(self.root, text=" 普通用户登录 ", compound="bottom", fg="black",
                                     font=("微软雅黑", 10))
        self.button.bind("<Button-1>", lambda event: self.user_login())
        self.button.place(x=90, y=140)
        #管理员登录按钮
        self.button2 = tkinter.Button(self.root, text=" 服务器管理员登录 ", compound="bottom", fg="black",
                                     font=("微软雅黑", 10))
        self.button2.bind("<Button-1>", lambda event: self.manager_login())
        self.button2.place(x=300, y=140)
        # 通过标签组件，实时显示信息 ,self.content.set()
        self.title_text = tkinter.StringVar()
        self.label1 = tkinter.Message(self.root, textvariable=self.title_text, width=400,
                                      font=("微软雅黑", 18))
        self.label1.place(x=70, y=20)
        self.title_text.set("     分布式文件系统  客户端")

        self.title_text2 = tkinter.StringVar()
        self.label2 = tkinter.Message(self.root, textvariable=self.title_text2, width=400,
                                      font=("微软雅黑", 10))
        self.label2.place(x=140, y=80)
        self.title_text2.set("")

        self.title_text3 = tkinter.StringVar()
        self.label3 = tkinter.Message(self.root, textvariable=self.title_text3, width=400,
                                      font=("微软雅黑", 10))
        self.label3.place(x=180, y=110)

        self.title_text4 = tkinter.StringVar()
        self.label4 = tkinter.Message(self.root, textvariable=self.title_text4, width=400,
                                      font=("微软雅黑", 10))
        self.label4.place(x=20, y=200)
        self.title_text4.set("说明：①登录前请先运行masterServer.py开启服务器，否则无法登录\n"
                             "        ②两个登录方式具有不同的权限，服务器管理员登录可额外查看集群情况和创建节点，为了方便展示，没有设置账户密码\n"
                             "        ③创建节点前，需要确保运行至少一个slaveServer.py以运行从服务器管理集群\n"
                             "        ")

        self.root.mainloop()
    #操作界面
    def mainForm2(self):
        self.root.destroy()
        self.root2 = tkinter.Tk()
        if self.id=="manager":
            self.root2.title("分布式文件系统客户端 管理员权限")
        else:
            self.root2.title("分布式文件系统客户端 普通用户权限")
        self.root2.geometry("650x350")
        #self.roo2.maxsize(600, 300)
        #命令行
        self.e1=tkinter.Entry(self.root2,width=50)
        self.e1.place(x=20,y=30)
        #执行按钮
        self.button2_1 = tkinter.Button(self.root2, text=" 执行 ", compound="bottom", fg="black",
                                     font=("微软雅黑", 10))
        self.button2_1.bind("<Button-1>", lambda event: self.execute())
        self.button2_1.place(x=420, y=20)
        #刷新按钮
        self.button2_2 = tkinter.Button(self.root2 , text=" 刷新 ", compound="bottom", fg="black",
                                        font=("微软雅黑", 10))
        self.button2_2.bind("<Button-1>", lambda event: self.refresh())
        self.button2_2.place(x=500, y=20)
        # 刷新按钮
        # self.button2_3 = tkinter.Button(self.root2, text=" 刷新 ", compound="bottom", fg="black",
        #                                 font=("微软雅黑", 10))
        # self.button2_3.bind("<Button-1>", lambda event: self.refresh())
        # self.button2_3.place(x=450, y=180)
        #执行情况标签
        self.label_1 = tkinter.Message(self.root2 , text="执行情况:", width=100,
                                      font=("微软雅黑", 10))
        self.label_1.place(x=20,y=80)
        #执行情况内容标签
        self.text2 = tkinter.StringVar()
        self.label_2 = tkinter.Message(self.root2, textvariable=self.text2, width=150,
                                       font=("微软雅黑", 8))
        self.label_2.place(x=20, y=110)
        self.text2.set("")
        #使用说明
        if self.id=="manager":
            # group标签
            self.label_3 = tkinter.Message(self.root2, text="集群", width=100,
                                           font=("微软雅黑", 10))
            self.label_3.place(x=220, y=80)
            # 节点数量标签
            self.label_4 = tkinter.Message(self.root2, text="节点数量", width=100,
                                           font=("微软雅黑", 10))
            self.label_4.place(x=300, y=80)
            # 连接状态标签
            self.label_5 = tkinter.Message(self.root2, text="连接状态", width=100,
                                           font=("微软雅黑", 10))
            self.label_5.place(x=380, y=80)

            self.label_9 = tkinter.Message(self.root2, text="leader's id", width=100,
                                           font=("微软雅黑", 10))
            self.label_9.place(x=460, y=80)


            self.text_group = tkinter.StringVar()
            self.label_6 = tkinter.Message(self.root2, textvariable=self.text_group, width=100,
                                           font=("微软雅黑", 10))
            self.label_6.place(x=220, y=110)
            self.text_node = tkinter.StringVar()
            self.label_7 = tkinter.Message(self.root2, textvariable=self.text_node, width=100,
                                           font=("微软雅黑", 10))
            self.label_7.place(x=300, y=110)
            self.text_connect = tkinter.StringVar()
            self.label_8 = tkinter.Message(self.root2, textvariable=self.text_connect, width=100,
                                           font=("微软雅黑", 10))
            self.label_8.place(x=380, y=110)

            self.text_leader= tkinter.StringVar()
            self.label_10 = tkinter.Message(self.root2, textvariable=self.text_leader, width=100,
                                           font=("微软雅黑", 10))
            self.label_10.place(x=460, y=110)

            self.text3 = tkinter.StringVar()
            self.label_11 = tkinter.Message(self.root2, textvariable=self.text3, width=400,
                                            font=("微软雅黑", 8))
            self.label_11.place(x=220, y=180)
            self.text3.set("指令说明：\ncreate node [数字1] [数字2] :\t在集群[数字1]创建[数字2]个节点node\n"
                           "ls :\t\t查看分布式系统中有哪些文件\n"
                           "create file [文件名] :\t在分布式文件系统中创建该文件\n"
                           "read [文件名] :\t从分布式系统中读取文件\n"
                           "write [文件名] :\t向分布式系统写入文件，覆盖相同文件名文件\n"
                           "rename [文件名1] [文件名2] :\t将[文件名1]的文件名字改成[文件名2]\n"
                           "delete [文件名] :\t删除分布式系统中的文件")
        else:
            self.text3 = tkinter.StringVar()
            self.label_11 = tkinter.Message(self.root2, textvariable=self.text3, width=400,
                                            font=("微软雅黑", 8))
            self.label_11.place(x=220, y=80)
            self.text3.set("指令说明：\n"
                           "ls :\t\t查看分布式系统中有哪些文件\n"
                           "create file [文件名] :\t在分布式文件系统中创建该文件"
                           "read [文件名] :\t从分布式系统中读取文件\n"
                           "write [文件名] :\t向分布式系统写入文件，覆盖相同文件名文件\n"
                           "rename [文件名1] [文件名2] :\t将[文件名1]的文件名字改成[文件名2]\n"
                           "delete [文件名] :\t删除分布式系统中的文件")

        #执行情况
        # self.text2 = tkinter.StringVar()
        # self.label_2 = tkinter.Message(self.root2, textvariable=self.text2, width=40,
        #                               font=("微软雅黑", 10))
        # self.label_2.place(x=220, y=200)
        # self.text2.set("执行情况:")
        self.status=True
        self.refresh()
        self.root2.mainloop()
    #界面刷新
    def refresh(self):  #刷新显示信息
        if self.status==False:   #还没打开UI界面，就不需要刷新了
            return
        self.e1.delete(0, 50)
        if self.id=="manager":
            self.text_group.set(self.groups_name[0]+'\n'+self.groups_name[1]+'\n'+self.groups_name[2])
            self.text_node.set(self.groups_num[0] + '\n' + self.groups_num[1] + '\n' + self.groups_num[2])
            self.text_connect.set(self.groups_state[0] + '\n' + self.groups_state[1] + '\n' + self.groups_state[2])
            self.text_leader.set(self.leader[0]+ '\n' + self.leader[1] + '\n' + self.leader[2])
    #执行时输入的命令
    def execute(self):
        s=self.e1.get()#获取用户输入的指令字符串
        s_l=s.split()#按空格分割字符得到指令各单词
        try:
            if s_l[0] == "ls":#根据指令首单词类型，调用相关处理函数
                thread=threading.Thread(target=self.list_handle,args=())
                thread.start()
            elif s_l[0]=="create" and s_l[1]=="node":
                data = {"type": "create_node", "group_id": 1, "num": 3}
                data["group_id"]=int(s_l[2])
                data["num"] = int(s_l[3])
                if int(s_l[2])<1 or int(s_l[2])>3:
                    self.text2.set("语法错误！")
                    return
                if self.id!="manager":
                    self.text2.set("权限不足，无法创建节点")
                data = json.dumps(data).encode()
                self.client.send(data)  # 创建节点
                self.text2.set("group%d成功创建%d节点\n\n在下次操作前请等待约几秒时间，直到leader选举产生"%(int(s_l[2]),int(s_l[3])))
            elif s_l[0]=="create" and s_l[1]=="file":
                print(s_l[2])
                self.create_handle(s_l[2])
                self.text2.set("执行成功")
            elif s_l[0]=="write":
                self.write_handle(s_l[1])
                self.text2.set("执行成功")
            elif s_l[0]=="read":
                self.read_handle(s_l[1])
                self.text2.set("执行成功")
            elif s_l[0]=="rename":
                self.rename_handle(s_l[1],s_l[2])
                self.text2.set("执行成功")
            elif s_l[0]=="delete":
                self.delete_handle(s_l[1])
                self.text2.set("执行成功")
            else:
                self.text2.set("无效指令!")
        except Exception:
            logger.info("error")
            self.text2.set("语法错误！")
            pass
    #监听服务端响应
    def listen_handle(self):  #监听服务端响应
        maxsize=self.maxsize
        while True:
            response = {'type': 'none'}
            try:
                # data= self.client.recv(maxsize)
                # data= json.loads(data)
                # if data["type"]=="group_info":     #slaves通知
                #      self.groups=data["group_id"]
                #      # for k in self.groups:
                #      #     self.groups_state[k-1]="连接成功"
                #      #     self.groups_num[k-1]=str(data["group_node"][k])
                #      for i in range(len(self.groups)):
                #          self.groups_state[self.groups[i] - 1] = "连接成功"
                #          self.groups_num[self.groups[i] - 1] = str((data["group_node"])[i])
                data, addr = self.rpc_endpoint2.recv()
                if data["type"]=="group_info":     #slaves通知
                    #print(1)
                    self.groups = data["group_id"]
                    for i in range(len(self.groups)):
                        self.groups_state[self.groups[i] - 1] = "连接成功"
                        self.groups_num[self.groups[i] - 1] = str((data["group_node"])[i])
                    self.refresh()
                if data["type"]=="leader_info":
                    self.leader[data["group_id"]-1]=data["id"]
                    self.refresh()
                    self.text2.set("执行成功：leader选举产生，可继续执行")

            except Exception:
                break
        self.client.close()
    #用户登录操作
    def user_login(self):
        self.id="user"
        self.title_text3.set("登录中...")
        try:
            self.client.connect((conf["master_ip"], conf["master_port"]))  # 发起三次握手第一次握手
            data = {"type": "ID_check", "character": "user","addr":self.addr,"addr2":self.addr2}
            data = json.dumps(data).encode()#打包数据
            self.client.send(data)  # 发送认证数据包，完成登录验证
            thread = threading.Thread(target=self.listen_handle, args=())#创建线程监听其它发来的请求
            thread.start()

        except Exception:
            self.title_text3.set("登录失败，请重试!")
            self.mainForm2()
            return
        self.mainForm2()
    #管理员登录操作
    def manager_login(self):
        self.id="manager"
        self.title_text3.set("登录中...")
        try:
            thread = threading.Thread(target=self.listen_handle, args=())
            thread.start()
            self.client.connect((conf["master_ip"], conf["master_port"]))  # 发起三次握手第一次握手
            data = {"type": "ID_check", "character": "manager","addr":self.addr,"addr2":self.addr2}
            data = json.dumps(data).encode()
            self.client.send(data)  # 完成登录验证
            #time.sleep(10)
            print(1)
            # rpc_endpoint.send(data, (conf["master_ip"], conf["master_port"]))  #直接发送到master结点
        except Exception:
            self.title_text3.set("登录失败，请重试!")
            self.mainForm2()
            return
        self.mainForm2()
    #主体运行代码
    def run(self):
        self.mainForm()
        # self.client.connect((conf["master_ip"], conf["master_port"]))  # 发起三次握手第一次握手
        # data = {"type": "ID_check", "character": "visitor"}
        # data = json.dumps(data).encode()
        # self.client.send(data)  # 完成登录验证
        # # rpc_endpoint.send(data, (conf["master_ip"], conf["master_port"]))  #直接发送到master结点
        #
        # time.sleep(1)
        # data = {"type": "create_node", "group_id": 1, "num": 3}
        # data = json.dumps(data).encode()
        # self.client.send(data)  # 创建节点
        # time.sleep(1)
        #self.write_handle("1.txt")
        #self.read_handle("s1.txt")
        #self.delete_handle("s1.txt")
        #self.rename_handle("1.txt","3.txt")
        # ans=self.list_handle()
        # print(ans)
        # while (True):
        #     pass
        # try:
        #     data, _ = client1.recv(maxsize)    #获取组
        #     group_meta = data['meta']
        #     print(group_meta)
        # except Exception:
        #     traceback.print_exc()   #输出不能连接的信息
        #     sys.exit(1)

        # while True:
        #     try:
        #         res, _ = rpc_endpoint.recv(timeout=2)
        #         print("receive: commit success", res)
        #     except KeyboardInterrupt:
        #         rpc_endpoint.close()
        #         return 0
        #     except Exception:
        #         traceback.print_exc()
        #
        #     addr = random.choice(group_meta["nodes"])
        #     data = {"type": "client_append_entries", "timestamp": int(time.time())}
        #     print("send: ", data)
        #
        #     rpc_endpoint.send(data, addr)
        #
        #     time.sleep(10)
def main() -> int:
    client=Client()
    client.run()
    return  0
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(funcName)s [line:%(lineno)d]\n%(message)s",
    )
    sys.exit(main())
