#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-05-16 12:46:02
# @Author  : Zheng Zhilong (zhengzl0715@163.com)
# @Link    : 
# @Version : $Id$

import json
import log
from readfile_thread import ReadFileThread
from datahandler_thread import DataHandlerThread


class NfData(object):
    def __init__(self):
        self.file_path = None
        self.result_data = []
        self.readfile_thread = None
        self.datahandler_pool = [] #处理的线程池

    def data_ready_cb(self, data, data_name):
        datahandler = DataHandlerThread(data, data_name, self.result_data) #反正文件少，直接建新线程就行，

    def main(self):
        # config
        config_file_name = 'config.json'
        try:
            with open(config_file_name) as config_data:
                configs = json.load(config_data)
        except:
            print('failed to load config from config.json.')
            return

        # log init
        log.initialize_logging(configs['enable_log'].lower() == 'true')
        log.info('Main: start')

        # threads
        self.file_path = configs['file_path']
        self.readfile_thread = ReadFileThread(self.file_path, self.data_ready_cb)

        #self.readfile_thread.start()

        # keyboard
        try:
            print("enter 'q' to quit")
            while input() != 'q':
                if not self.client.running or not self.server.running:
                    break
        except KeyboardInterrupt:
            pass

        # quit & clean up
        self.readfile_thread.stop()
        log.info('Main: bye')


if __name__ == '__main__':
    nfdata = NfData()
    nfdata.main()
