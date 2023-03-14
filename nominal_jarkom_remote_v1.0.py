import mysql.connector
import pandas as pd
import datetime
import numpy as np

connection1 = mysql.connector.connect(host='localhost',
                                         database='dbrecon_20220217',
                                         user='root',
                                         password='', charset='utf8')
connection2 = mysql.connector.connect(host='localhost',
                                         database='recon2',
                                         user='root',
                                         password='', charset='utf8')

query_select = "SELECT * FROM tb_remote"
cursor = connection1.cursor()
cursor.execute(query_select)
records = cursor.fetchall()

query_insert = 'INSERT IGNORE INTO tb_remote_copy VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
cursor = connection2.cursor()
cursor.executemany(query_insert, records)
connection2.commit()

query_select = "SELECT * FROM tb_jarkom"
cursor = connection1.cursor()
cursor.execute(query_select)
records = cursor.fetchall()

query_insert = 'INSERT IGNORE INTO tb_jarkom_copy VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
cursor = connection2.cursor()
cursor.executemany(query_insert, records)
connection2.commit()

cursor.close()
connection1.close()
connection2.close()