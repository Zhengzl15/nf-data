#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-05-15 20:57:31
# @Author  : Zheng Zhilong (zhengzl0715@163.com)
# @Link    : 
# @Version : $Id$

import socket
import threading
import queue
import log


class DataHandlerThread(threading.Thread):
    """处理文件"""

    def __init__(self, data, data_name, result_data):
        """构造函数

        Args:
            data: 代处理的数据
            data_name: 要处理数据的名字，格式如20151108_www.amazon.com
            result_data: 一个全局的数据，用来存放最终所有文件处理的结果
        """
        super().__init__()
        self.set_data(data, data_name, result_data)

        self.running = True
        self.idle = True #如果空闲就可以处理

    def run(self):
        """按时段分类数据，分为分钟，小时，天的变化

        Args:
            
        """
        while self.running:
            if not self.has_handled:
                self.idle = False
                log.info('handle file %s' % (self.data_name))
                self.__pre_handle() #先预处理数据
                self.idle = True
                self.has_handled = True

    def __pre_handle(self):
        """预处理数据

        将数据分为7元组：
            [0] 时间
            [1] 连接持续时间
            [2] 协议
            [3] 源地址
            [4] 目的地址
            [5] 包数量
            [6] 总共字节数
        """
        self.date = self.data_name[0]
        self.tuple_data = []
        iter_range = range(1, len(self.data))
        for i in iter_range:
            line = self.data[i].split('\t')
            tmp_date = line.replace('-', '')
            if tmp_date == self.date:
                self.tuple_data.append((line[1], \
                                        line[2], \
                                        line[3], \
                                        line[4], \
                                        line[6], \
                                        line[7], \
                                        line[8])) 

    def set_data(self, data, data_name, result_data)  :
        """设置要处理的数据

            参数意义等同于构造函数

        """
        self.data = data
        self.data_name = data_name
        self.result_data = result_data
        self.has_handled = False

    def is_idle(self):
        """返回当前线程是否空闲, 若空闲，则可以继续处理数据

        """
        return self.is_idle

    def stop(self):
        self.running = False 
        self.join(timeout=3)
