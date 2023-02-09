#!/usr/bin/env python
# coding: utf-8


class Config(object):
    ip = "localhost"   #在本机上模拟分布式环境
    mport = 8800  # master port
    sport = 9999  # slave port
    cport = 11000  # client1 port

    master_path = "data/master/"   #存储路径
    slave_path = "data/slave/"
    node_path = ""

class DevConfig(Config):
    env = "DEV"


class ProdConfig(Config):
    env = "PROD"


config = {
    "DEV": DevConfig,
    "PROD": ProdConfig
}
