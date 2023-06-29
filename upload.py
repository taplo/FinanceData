# -*- coding: utf-8 -*-
"""
Created on 2023-05-19 09:10:00
通过阿里云上传数据库文件
@author: Administrator
"""
import shutil, os
import datetime

import configparser

from aligo import Aligo, EMailConfig


def upload_file():
    conf = configparser.ConfigParser()
    conf.read('../config.ini',encoding="utf-8-sig")
    
    target_email = conf.get('anapro', 'target_email')
    username = conf.get('anapro', 'user')
    password = conf.get('anapro', 'password')
    host = conf.get('anapro', 'host')
    port = int(conf.get('anapro', 'port'))
    
    email_config = EMailConfig(email=target_email, user=username, password=password,
                           host=host, port=port)
    ali = Aligo(email=email_config)
    remote_folder = ali.get_folder_by_path('数据')
    
    # 按照日期复制文件
    filename = datetime.datetime.now().isoformat().split('.')[0] + '.db'
    filename = filename.replace(':', '-')
    '''旧的linux版复制文件
    cmd = "cp /workdir/default.db /workdir/" + filename
    result = os.system(cmd)
    print(result)
    '''
    shutil.copy('/workdir/default.db', '/workdir/'+filename)
    print('已复制'+filename+'文件。')
    result = ali.upload_file(file_path='/workdir/'+filename, parent_file_id=remote_folder.file_id)
    print(result)
    '''旧的linux版删除文件
    result = os.system("rm /workdir/" + filename)
    print(result)
    '''
    os.remove('/workdir/'+filename)
    print('已删除'+filename+'文件。')
    
if __name__ == '__main__':

    upload_file()