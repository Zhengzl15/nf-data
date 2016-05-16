#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-05-16 12:46:02
# @Author  : Zheng Zhilong (zhengzl0715@163.com)
# @Link    : 
# @Version : $Id$

import json
import log
import sys
from readfile_thread import ReadFileThread
from datahandler_thread import DataHandlerThread


class NfData(object):
    def __init__(self):
        self.file_path = None
        self.result_data = []   #用于存放全局数据，
        self.readfile_thread = None
        self.datahandler_pool = [] #处理的线程池

    def data_ready_cb(self, data, data_name):
        self.__dispatch(data, data_name)
        
    def __dispatch(self, data, data_name):
        """负责任务的分发
            因为当前需求很少, 所以一个函数就可以了

        """
        handler = None
        handler_idle = False
        for handler in self.datahandler_pool:
            if handler.is_idle():
                handler_idle = True
                break
        if handler_idle:
            handler.set_data(data, data_name, self.result_data)
        else:   #新申请一个线程
            thread = DataHandlerThread(data, data_name, self.result_data)
            thread.start()
            self.datahandler_pool.append(thread)
            log.info('Current number of data handlers is %d' % (len(self.datahandler_pool)))

    def main(self):
        # config
        config_file_name = 'config.json'
        try:
            with open(config_file_name) as config_data:
                configs = json.load(config_data)
        except Exception as e:
            print(e)
            print('failed to load config from config.json.')
            return

        # log init
        log.initialize_logging(configs['enable_log'].lower() == 'true')
        log.info('Main: start')

        # threads
        self.file_path = configs['file_path']
        self.readfile_thread = ReadFileThread(self.file_path, self.data_ready_cb)
        init_thread = DataHandlerThread(None, None, self.result_data)
        init_thread.start()
        self.datahandler_pool.append(init_thread) #先在线程池中初始化一个

        self.readfile_thread.start()

        # keyboard
        try:
            print("enter 'q' to quit")
            while input() != 'q':
                    break
        except KeyboardInterrupt:
            pass

        # 退出
        self.readfile_thread.stop()
        for handler in self.datahandler_pool:
            handler.stop()

        log.info('Main: bye')

if __name__ == '__main__':
    nfdata = NfData()
    nfdata.main()
