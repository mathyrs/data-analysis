# -*- coding: utf-8 -*-
"""
Created on Wed Nov 14 14:23:44 2018

read csv files of an Directory & insert into mysql data table

@author: mathyrs
"""

# -*- coding: utf-8 -*-
#!/usr/bin/env python
#
#Author: Liu6
import zipfile
import csv
import codecs
import pymysql
import datetime
import os
from ftplib import FTP
import codecs
#import pandas as pd
import sys
import imp
import re

imp.reload(sys)


def getYesterday() :
    now = datetime.datetime.now()
    date = now + datetime.timedelta(days = -1)
    return date.strftime('%Y%m%d')



#设置批量处理数据数量
BATCH_LINE = 10000


encoding='utf-8'

columns = 'table_columns like name varchar(255), id int(8), addr varchar(255)';

def ftpDownload() :

    #创建ftp对象实例
    ftp = FTP()

    ftp.connect(FTPIP, FTPPORT)
    #通过账号和密码登录FTP服务器
    ftp.login(USERNAME,USERPWD)

    #如果参数 pasv 为真，打开被动模式传输 (PASV MODE) ，
    #否则，如果参数 pasv 为假则关闭被动传输模式。
    #在被动模式打开的情况下，数据的传送由客户机启动，而不是由服务器开始。
    #这里要根据不同的服务器配置
#	ftp.set_pasv(0)

    #在FTP连接中切换当前目录
#	CURRTPATH= "/home1/ftproot/ybmftp/testupg/payment"
#	ftp.cwd(CURRTPATH)

    #为准备下载到本地的文件，创建文件对象
    f = open(DownLocalFilename, 'wb')
    #从FTP服务器下载文件到前一步创建的文件对象，其中写对象为f.write，1024是缓冲区大小
    ftp.retrbinary('RETR ' + DownRoteFilename , f.write , 1024)

    #关闭下载到本地的文件
    #提醒：虽然Python可以自动关闭文件，但实践证明，如果想下载完后立即读该文件，最好关闭后重新打开一次
    f.close()

    #关闭FTP客户端连接
    ftp.close()


def mysql_database(L1, table_name, SQL_FILEDS):
    db = pymysql.connect("ip","user","passwd","database",use_unicode=True, charset="utf8")
    cursor = db.cursor()
    cursor.execute('SET NAMES utf8;')
    cursor.execute('SET CHARACTER SET utf8;')
    cursor.execute('SET character_set_connection=utf8;')
    #for row in L1 :
    #	print row
    #print SQL_FILEDS

    if (table_exists(cursor, table_name) == 1):
        sql_parm=''
        for s in  SQL_FILEDS :
            sql_parm +='%s,'

        sql = 'insert into ' + table_name + ' VALUES ('+sql_parm[:-1]+')'
        cursor.executemany(sql,tuple(L1))
    else:
        createsql = 'create table ' + table_name + ' (' + columns + ')';
        cursor.execute(createsql)
        sql_parm=''
        for s in  SQL_FILEDS :
            sql_parm +='%s,'

        sql = 'insert into ' + table_name + ' VALUES ('+sql_parm[:-1]+')'
        cursor.executemany(sql,tuple(L1))

    #test_all_count = int(cursor.rowcount)
    #test_all = cursor.fetchall()

    db.commit()
    cursor.close()
    db.close()


def table_exists(cursor,table_name):     #这个函数用来判断表是否存在
    sql = "show tables;"
    cursor.execute(sql)
    tables = [cursor.fetchall()]
    table_list = re.findall('(\'.*?\')',str(tables))
    table_list = [re.sub("'",'',each) for each in table_list]
    if table_name in table_list:
        return 1    #存在返回1
    else:
        return 0    #不存在返回

def readCSVFile(file, table_name) :
    with codecs.open(file,'rb','utf-8') as csvfile:
#		with open(file,'rb') as csvfile:
        spamreader = csv.reader(csvfile)
        line_num = 0
        L1 = []
        SQL_FILEDS = []
        for row in spamreader:
            line_num=line_num+1;
            if line_num ==1 :
					#print ', '.join(row)
					#存放字段前先清空
					#SQL_FILEDS =[]
                for s in row:
                    if (s != ''):
                        SQL_FILEDS.append(s)
            if (len(row) != len(SQL_FILEDS)):
                print('ERROR, row columns does not equal to SQL_FILEDS');
                print(row)
            if line_num >1 and len(row) > 1:
                L1.append(tuple(row))
#					print L1
					#达到批量处理行数之后批量入库
            if len(L1) >= BATCH_LINE :
                mysql_database(L1, table_name, SQL_FILEDS)
                L1  =[]
				#测试时候只读取几行
				#if line_num >10 :
				#	break
			#循环读取数据结束，处理剩余未达到批量处理的数组对象
        if len(L1) > 0:
            mysql_database(L1, table_name, SQL_FILEDS)
            L1 =[]



def printPath(level, path):
    '''''
    打印一个目录下的所有文件夹和文件
    '''
    # 所有文件夹，第一个字段是次目录的级别
    dirList = []
    # 所有文件
    fileList = []
    # 返回一个列表，其中包含在目录条目的名称(google翻译)
    files = os.listdir(path)
    # 先添加目录级别
    dirList.append(str(level))
    for f in files:
        if(os.path.isdir(path + '/' + f)):
            # 排除隐藏文件夹。因为隐藏文件夹过多
            if(f[0] == '.'):
                pass
            else:
                # 添加非隐藏文件夹
                dirList.append(f)
        if(os.path.isfile(path + '/' + f)):
            # 添加文件
            fileList.append(f)
    # 当一个标志使用，文件夹列表第一个级别不打印
    i_dl = 0
    for dl in dirList:
        if(i_dl == 0):
            i_dl = i_dl + 1
        else:
            # 打印至控制台，不是第一个的目录
            print('-' * (int(dirList[0])), dl)
            # 打印目录下的所有文件夹和文件，目录级别+1
            printPath((int(dirList[0]) + 1), path + '/' + dl)
    for fl in fileList:
        # 打印文件
        print('-' * (int(dirList[0])), fl)
        print(path + '\\' + fl + ' table_name: ' + fl.split('.')[0])
        # 随便计算一下有多少个文件
        readCSVFile(path + '\\' + fl, fl.split('.')[0])

if __name__ == '__main__':
    printPath(1, 'E:\\documents\\')
