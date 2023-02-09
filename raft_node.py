# !/usr/bin/env python
# coding: utf-8
# 创建后需等待连接

import os
import sys
import json
import threading
import time
import random
import logging
import traceback
from turtle import st

from log import Log
from rpc import Rpc
from config import config

logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
logger.propagate = False  # 避免重复打印

#运行raft协议的节点
class Node(object):
    def __init__(self, meta: dict):
        self.role = 'follower'  # 初始角色
        self.group_id = meta['group_id']  # 创建时赋予组id
        self.id = meta['id']  # 自身id
        self.addr = meta['addr']  # ip地址和端口
        self.peers = meta['peers']  # 对等结点情况
        self.slave_addr=meta["slave_addr"]

        self.conf = self.load_conf()  # 从config文件获取配置
        self.path = os.path.join(self.conf.node_path,"node"+self.id+"/")
        if not os.path.exists(self.path):
            os.makedirs(self.path)

        # persistent state
        self.current_term = 0
        self.voted_for = None

        # init persistent state
        self.load()
        # 日志
        logname = os.path.join(self.path,self.group_id + '_' + self.id + '_log.json')
        self.log = Log(logname)

        # volatile state
        # rule 1, 2

        # volatile state on leaders
        # rule 1, 2
        self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers}
        self.match_index = {_id: -1 for _id in self.peers}

        self.commit_index = self.log.last_log_index
        self.last_applied = self.log.last_log_index
        #print("初始时",self.commit_index,"",self.last_applied)

        # append entries
        self.leader_id = None

        # request vote
        self.vote_ids = {_id: 0 for _id in self.peers}

        # client1 request
        self.client_addr = None

        # tick
        self.wait_ms = (10, 20)
        self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)
        self.next_heartbeat_time = 0

        # rpc
        self.rpc_endpoint = Rpc(self.addr, timeout=2)

        # log
        fmt = logging.Formatter('%(asctime)s %(levelname)s %(name)s %(funcName)s [line:%(lineno)d] %(message)s')
        handler = logging.FileHandler(os.path.join(self.path,self.group_id + '_' + self.id + '.log'), 'a')
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    @staticmethod
    def load_conf(): #加载配置文件
        env = os.environ.get("env")
        conf = config[env] if env else config['DEV']
        return conf
    def load(self):  #加载日志
        filename = os.path.join(self.path, self.group_id + '_' + self.id + '_persistent.json')
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                persistent = json.load(f)
            self.current_term = persistent['current_term']
            self.voted_for = persistent['voted_for']
        else:
            self.save()
    def save(self):  #记录最新任期和投票
        persistent = {'current_term': self.current_term,
                      'voted_for': self.voted_for}

        filename = os.path.join(self.path, self.group_id + '_' + self.id + '_persistent.json')
        with open(filename, 'w') as f:
            json.dump(persistent, f, indent=4)
    def redirect(self, data: dict, addr: tuple[str, int]) -> dict:#转发数据
        if not data:
            return {}

        if data.get('type') == 'client_append_entries':
            if self.role != 'leader':
                if self.leader_id:
                    logger.info(f"redirect client_append_entries to leader: {self.leader_id}")
                    self.rpc_endpoint.send(data, self.peers.get(self.leader_id))
                return {}
            else:
                self.client_addr = (addr[0], self.conf.cport)
                # logger.info("client1 addr " + self.client_addr[0] +'_' +str(self.client_addr[1]))
                return data

        if data.get('dst_id') != self.id:
            logger.info(f"redirect to: {data.get('dst_id')}")
            self.rpc_endpoint.send(data, self.peers.get(data.get('dst_id')))
            return {}

        return data
    def append_entries(self, data: dict) -> bool:#一次可能包含用户操作请求的由leader发来的心跳
        response = {'type': 'append_entries_response',
                    'src_id': self.id,
                    'dst_id': data.get('src_id'),
                    'term': self.current_term,
                    'success': False}
        # append_entries rule 1
        if data.get('term') < self.current_term:   #term比自己还小，不是最新的
            logger.info('1. success = False: smaller term')
            logger.info(f"  send append_entries_response to leader: {data.get('src_id')}")
            response['success'] = False
            self.rpc_endpoint.send(response, self.peers.get(data.get('src_id')))
            return False

        if not data.get("entries"):   #没有找到项，说明只是心跳
            logger.info("heartbeat")
        # if len(data.get("entries"))==0:   #没有找到项，说明只是心跳
        #     logger.info("heartbeat")
        else:
            print(data)
            # append_entries rule 2, 3
            prev_log_index = data.get('prev_log_index', -1)
            prev_log_term = data.get('prev_log_term')

            if prev_log_term != self.log.get_log_term(prev_log_index) or prev_log_index!=self.log.last_log_index: #term对不上
                logger.info('2. success = False: index not match or term not match')
                response['success'] = False
                logger.info('3. log delete_entries')
                self.log.delete_entries(prev_log_index)
            else:
                # append_entries rule 4
                logger.info('4. success = True')
                logger.info('   log append_entries')
                response['success'] = True
                self.log.append_entries(prev_log_index, data.get('entries', []))
                data2 = data['entries']

            logger.info(f"   send append_entries_response to leader: {data.get('src_id')}")
            self.rpc_endpoint.send(response, self.peers.get(data.get('src_id')))

        # append_entries rule 5
        leader_commit = data.get('leader_commit')
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, self.log.last_log_index)
            logger.info(f"5. commit_index = {str(self.commit_index)}")

        # set leader_id
        self.leader_id = data.get('leader_id')
        return True
    def handle_write(self,filename,mode,content):#处理写请求
        if mode=="new":
            with open(self.path+filename, "w+") as out_file:
                out_file.write(content)
        elif mode=="append":
            with open(self.path+filename, "a+") as out_file:
                out_file.write(content)
        logger.info("finish write %s"%filename)
    def handle_read(self,filename,src_addr):#处理读请求
        response = {"type": "read_node_response", "mode": "new", "filename": filename, "content": ""}
        with open(self.path+filename, "r+") as in_file:
            response["content"] = in_file.read(1024)
            while response["content"]:
                self.rpc_endpoint.send(response, src_addr)
                # print(f"Sent {data!r}")
                response["content"] = in_file.read(1024)
                response["mode"] = "append"
            response["mode"]="finish"
            self.rpc_endpoint.send(response, src_addr)
    def handle_delete(self,filename,src_addr):#处理删除请求
        if os.path.exists(self.path+filename):
            os.remove(self.path+filename)
        #print(2)
    def handle_rename(self,filename,new_name):##处理重命名请求
        if os.path.exists(self.path+filename):
            os.rename(self.path+filename,self.path+new_name)
    def handle_create(self,filename,src):#处理创建文件请求
        with open(self.path + filename, "w+") as out_file:
            pass
    def request_vote(self, data: dict) -> bool:#响应有投票请求
        response = {'type': 'request_vote_response',
                    'src_id': self.id,
                    'dst_id': data['src_id'],
                    'term': self.current_term,
                    'vote_granted': False}

        # request_vote rule 1
        if data.get('term') < self.current_term:
            logger.info('1. success = False: smaller term')
            logger.info(f"   send request_vote_response to candidate: {data.get('src_id')}")
            response['vote_granted'] = False
            self.rpc_endpoint.send(response, self.peers.get(data.get('src_id')))
            return False

        # request_vote rule 2
        last_log_index = data.get('last_log_index')
        last_log_term = data.get('last_log_term')

        if self.voted_for is None or self.voted_for == data.get('candidate_id'):
            if last_log_index >= self.log.last_log_index and last_log_term >= self.log.last_log_term:
                self.voted_for = data.get('src_id')
                self.save()
                response['vote_granted'] = True
                logger.info('2. success = True: candidate log is newer')
                logger.info(f"   send request_vote_response to candidate: {data['src_id']}")
                self.rpc_endpoint.send(response, self.peers.get(data.get('src_id')))

                return True
            else:
                self.voted_for = None
                self.save()
                response['vote_granted'] = False
                logger.info('2. success = False: candidate log is older')
                logger.info(f"   send request_vote_response to candidate: {data['src_id']}")
                self.rpc_endpoint.send(response, self.peers.get(data.get('src_id')))
                return False
        else:
            response['vote_granted'] = False
            logger.info(f"2. success = False: has vated for: {self.voted_for}")

            return True
    def all_do(self, data: dict):  #所有节点每轮循环都执行的操作
        logger.info(f"all {self.id}".center(100, '-'))
        # all rule 1
        while self.commit_index > self.last_applied:
            #print("大于 %d %d"%(self.commit_index,self.last_applied))
            self.last_applied += 1
            if self.role=="leader":
                print(self.log.last_log_index)
            #self.last_applied = self.commit_index
            data2=self.log.get_entries(self.last_applied)
            if len(data2)==0:
                break
            if len(data2)!=0:
            #     break
                if data2[0]["op"] == "write":
                    # p = threading.Thread(target=self.handle_write,
                    #                      args=(data2[0]["filename"], data2[0]["mode"], data2[0]["content"]))
                    # p.start()
                    self.handle_write(data2[0]["filename"],data2[0]["mode"], data2[0]["content"])
                    # self.handle_write(data["filename"],data["mode"],data["content"])
                # if data2[0]["op"] == "read":
                #     p = threading.Thread(target=self.handle_read, args=(data2[0]["filename"], data2[0]["src_addr"]))
                #     p.start()
                if data2[0]["op"] == "delete":
                    p = threading.Thread(target=self.handle_delete, args=(data2[0]["filename"], data2[0]["src_addr"]))
                    p.start()
                if data2[0]["op"] == "rename":
                    p = threading.Thread(target=self.handle_rename, args=(data2[0]["filename"], data2[0]["new_name"]))
                    p.start()
                if data2[0]["op"] == "create":
                    p = threading.Thread(target=self.handle_create, args=(data2[0]["filename"], data2[0]["src_addr"]))
                    p.start()

            logger.info(f"1. last_applied = {str(self.last_applied)}")
            logger.info(f"   attention: need to apply to state machine")

        # all rule 2
        if data.get('term', -1) > self.current_term:
            logger.info("2. become follower")
            logger.info(f"   receive bigger term: {data.get('term')} > {self.current_term}")
            self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)
            self.role = 'follower'
            self.current_term = data.get('term')
            self.voted_for = None
            self.save()
            self.leader_id = None
    def follower_do(self, data: dict) -> bool:  #追随者
        logger.info(f"follower {self.id}".center(100, '-'))
        reset = False
        # follower rule 1
        if data.get('type') == 'append_entries': #日志加入
            logger.info("1. append_entries")
            logger.info(f"   receive from leader: {data.get('src_id')}")
            reset = self.append_entries(data)
        elif data.get('type') == 'request_vote':  #投票请求
            logger.info("1. request_vote")
            logger.info(f"   recevive from candidate: {data.get('src_id')}")
            reset = self.request_vote(data)

        # reset next_leader_election_time
        if reset:                           #重置选举时间
            logger.info('   reset next_leader_election_time')
            self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)
        # follower rule 2
        if time.time() > self.next_leader_election_time:  #超时，发起投票请求
            logger.info('1. become candidate')
            logger.info('   no request from leader or candidate')
            self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.id
            self.save()
            self.leader_id = None
            self.vote_ids = {_id: False for _id in self.peers}

            return True
    def candidate_do(self, data: dict) -> bool: #选举者操作
        logger.info(f"candidate {self.id}".center(100, '-'))
        # candidate rule 1
        for dst_id in self.peers:   #对所有同伴发送投票请求
            # if self.vote_ids.get(dst_id):
            #     continue
            request = {
                'type': 'request_vote',
                'src_id': self.id,
                'dst_id': dst_id,
                'term': self.current_term,
                'candidate_id': self.id,
                'last_log_index': self.log.last_log_index,
                'last_log_term': self.log.last_log_term
            }
            logger.info(f"1. send request_vote request to peer: {dst_id}")
            self.rpc_endpoint.send(request, self.peers.get(dst_id))

        # candidate rule 2
        if data.get('type') == 'request_vote_response':   #统计收到的票数
            logger.info(f"1. receive request_vote_response from follower: {data.get('src_id')}")
            self.vote_ids[data.get('src_id')] = data.get('vote_granted')
            vote_count = sum(list(self.vote_ids.values()))

            if vote_count >= len(self.peers) // 2:
                logger.info('2. become leader: get enougth vote')#新的leader通知从服务器自己的产生
                slave_info={"type":"inform_leader","leader_id":self.id,"leader_addr":self.addr}
                self.rpc_endpoint.send(slave_info,self.slave_addr)

                self.role = 'leader'
                self.voted_for = None
                self.save()
                self.next_heartbeat_time = 0
                self.next_index = {_id: self.log.last_log_index + 1 for _id in self.peers}
                self.match_index = {_id: self.log.last_log_index for _id in self.peers}
                return True

        # candidate rule 3
        elif data.get('type') == 'append_entries': #有数据项加入，说明leader产生，自己成为follower
            logger.info(f"1. receive append_entries request from leader: {data.get('src_id')}")
            logger.info('2. become follower')
            self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)
            self.role = 'follower'
            self.voted_for = None
            self.save()
            return

            # candidate rules: rule 4
        if time.time() > self.next_leader_election_time:  #再起发起请求
            logger.info('candidate: 1. leader_election timeout')
            logger.info('           2. become candidate')
            self.next_leader_election_time = time.time() + random.randint(*self.wait_ms)
            self.role = 'candidate'
            self.current_term += 1
            self.voted_for = self.id
            self.save()
            self.vote_ids = {_id: False for _id in self.peers}

            return
    def leader_do(self, data: dict): #领导者操作
        logger.info(f"leader {self.id}".center(100, '-'))
        # leader rule 1:
        if time.time() > self.next_heartbeat_time:
            self.next_heartbeat_time = time.time() + random.randint(0, 5)
            for dst_id in self.peers:
                request = {'type': 'append_entries',
                           'src_id': self.id,
                           'dst_id': dst_id,
                           'term': self.current_term,
                           'leader_id': self.id,
                           'prev_log_index': self.next_index[dst_id] - 1,
                           'prev_log_term': self.log.get_log_term(self.next_index[dst_id] - 1),
                           'entries': self.log.get_entries(self.next_index[dst_id]),
                           'leader_commit': self.commit_index}
                logger.info(f"1. send append_entries to peer: {dst_id}")
                self.rpc_endpoint.send(request, self.peers.get(dst_id))
        # leader rule 2
        if data.get('type') == 'client_append_entries':
            data['term'] = self.current_term   #加入当前自己的term进行存储
            self.log.append_entries(self.log.last_log_index, [data])
            logger.info('2. receive append_entries from client1')
            logger.info('   log append_entries')
            logger.info('   log save')
            if data["op"] == "read":
                p = threading.Thread(target=self.handle_read, args=(data["filename"], data["src_addr"]))
                p.start()
            return
        # leader rule 3
        if data.get('type') == 'append_entries_response':
            logger.info(f"1. receive append_entries_response from follower: {data.get('src_id')}")
            if data.get('success') == False:
                self.next_index[data.get('src_id')] -= 1
                logger.info('2. success = False, next_index - 1')
            else:   #日志跟上
                self.match_index[data.get('src_id')] = self.next_index.get(data.get('src_id'))
                self.next_index[data.get('src_id')] = self.log.last_log_index + 1
                logger.info(' 2. success = True')
                logger.info(f"  next_index = {str(self.next_index.get(data.get('src_id')))}")
                logger.info(f"  match_index = {str(self.match_index.get(data.get('src_id')))}")
        # leader rule 4    更新commit_index
        while True:
            N = self.commit_index + 1
            count = 0
            for _id in self.match_index:  #遍历每一个追随者
                if self.match_index[_id] >= N:
                    count += 1
                if count >= len(self.peers) // 2:
                    self.commit_index = N
                    logger.info('4. commit + 1')

                    if self.client_addr:
                        response = {'index': self.commit_index}
                        self.rpc_endpoint.send(response, self.client_addr)
                    break
            else:
                logger.info(f"4. commit = {str(self.commit_index)}")
                break
    def run(self):#主体运行框架
        while True:
            try:
                try:
                    data, addr = self.rpc_endpoint.recv()#接收集群下其它节点的信息
                except Exception as e:
                    data, addr = {}, None
                data = self.redirect(data, addr)#排除不是自己接收的消息
                self.all_do(data)
                if self.role == 'follower':
                    if self.follower_do(data):
                        continue
                if self.role == 'candidate':
                    if self.candidate_do(data):
                        continue
                if self.role == 'leader':
                    self.leader_do(data)
            except KeyboardInterrupt:
                self.rpc_endpoint.close()
                sys.exit(0)
            except Exception:
                traceback.print_exc()
                logger.info(traceback.format_exc())
def main() -> int:
    meta = {"group_id": "2",
            "id": "0",
            "addr": ("localhost", 10000),
            "peers": {"1": ("localhost", 10001), "2": ("localhost", 10002)}}
    node = Node(meta)

    node.run()
if __name__ == "__main__":
    sys.exit(main())