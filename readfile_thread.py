#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Date    : 2016-05-15 17:02:16
# @Author  : Zheng Zhilong (zhengzl0715@163.com)
# @Link    : 
# @Version : $Id$

import os
import glob
import threading
import log

class ReadFileThread(threading.Thread):
    """读文件的函数"""

    
    def __init__(self, file_path, data_ready_cb):
        """构造函数

            Args:
                file_path: 要读取的文件或者文件夹
                data_ready_cb: 读完文件的内容后的回调函数, 即交给回调函数自己处理, 该线程只负责文件的读取
        """
        
        super().__init__()
        self.file_path = file_path
        self.data_ready_cb = data_ready_cb

    def run(self):
        """主函数

        """
        
        log.info('ReadFile thread: start')
        #判断path是否存在，以及判断它是目录还是文件
        if not os.path.exist(self.file_path):
            log.error('File not found')
            raise FileNotFoundError()
        if os.path.isfile(self.file_path):
            self.read_file(self.file_path)
        elif os.path.isdir(self.file_path):
            self.read_dir(self.file_path)
        else:
            log.error('Broken file path')
            raise RuntimeError('Broken file path')
        log.info('ReadFile thread: end')

    def read_file(self, file_path):
        """传入文件时的处理函数
        """

        with open(file_path) as file:
            lines = file.readlines()    #直接一次把文件里的内容读到内存中来
            self.data_ready_cb(lines, file_path.split('/')[-1])   #通知回调函数，该数据已经读取到了
            log.info('Read %d lines in file %s ' % (len(lines), file_name))

    def read_dir(self, dir_path):
        """传入文件夹时的处理函数
        """
        log.info('Read files in dir: start')
        file_names = os.listdir(dir_path)  
        for file_name in file_names:
            if file_name.endswith('.nfdat'): #过滤出.nfdat的文件
                file_path = ''
                if dir_path.endswith('/')
                    file_path = dir_path + file_name
                else:
                    file_path = dir_path + '/' + file_name
                read_file(file_path)
            else:
                pass
        log.info('Read files in dir: end')

    def stop(self):
        self.running = False
        self.join(timeout=3)
