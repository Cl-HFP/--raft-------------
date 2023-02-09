# !/usr/bin/env python
# coding: utf-8

import os
import json
#entries 字典的列表
class Log(object):
    def __init__(self, filename: str):
        self.filename = filename   #日志文件名
        if os.path.exists(self.filename):  #如果有，则读取日志
            with open(self.filename, "r") as f:
                self.entries = json.load(f)
        else:
            self.entries = []
    @property  # 把方法当成属性
    def last_log_index(self):  #上一次日志的长度
        return len(self.entries) - 1

    @property
    def last_log_term(self):
        return self.get_log_term(self.last_log_index)

    def get_log_term(self, log_index: int) -> int:

        if log_index < 0 or log_index >= len(self.entries):#出错情况
            return -1
        else:      #
            return self.entries[log_index]["term"]    #上次记录最新的term数

    def get_entries(self, next_index: int) -> list[dict]:

        return self.entries[max(0, next_index):] #获取该行的日志记录

    def delete_entries(self, prev_log_index: int):#日志回溯，删除从这之后的
        if prev_log_index < 0 or prev_log_index >= len(self.entries):
            return
        self.entries = self.entries[: max(0, prev_log_index)]
        self.save()
    def append_entries(self, prev_log_index: int, entries: list[dict]):
        self.entries = self.entries[: max(0, prev_log_index + 1)] + entries
        self.save()
    def save(self):
        with open(self.filename, "w") as f:
            json.dump(self.entries, f, indent=4)
