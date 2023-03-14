import mysql.connector
import pandas as pd
import datetime
import numpy as np

db_connection = mysql.connector.connect(host='localhost',
                                         database='dbrecon_20220217',
                                         user='root',
                                         password='', charset='utf8')
query = "SELECT * FROM tb_remote"
df = pd.read_sql(sql=query, con=db_connection)
column_names = df.columns.values.tolist()
df = df.replace(np.nan, None)
nan_in_df = df.isnull().sum().sum()

query_insert = 'INSERT IGNORE INTO tb_remote_copy VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
pars = list(df.itertuples(index=False, name=None))
cursor = db_connection.cursor()
cursor.executemany(query_insert, pars)
db_connection.commit()

query = "SELECT * FROM tb_jarkom"
df = pd.read_sql(sql=query, con=db_connection)
column_names = df.columns.values.tolist()
df = df.replace(np.nan, None)
nan_in_df = df.isnull().sum().sum()

query_insert = 'INSERT IGNORE INTO tb_jarkom_copy VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
pars = list(df.itertuples(index=False, name=None))
cursor = db_connection.cursor()
cursor.executemany(query_insert, pars)
db_connection.commit()

cursor.close()
db_connection.close()