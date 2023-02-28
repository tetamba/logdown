import mysql.connector
import pandas as pd

db_connection = mysql.connector.connect(host='localhost',
                                         database='dbrecon_20220217',
                                         user='root',
                                         password='')
query = "SELECT * FROM tb_remote"
df = pd.read_sql(sql=query, con=db_connection)

query_insert = 'INSERT INTO tb_remote VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
# pars = df.values.tolist()
pars = list(df.itertuples(index=False, name=None))
# print(pars)
# exit()
cursor = db_connection.cursor()
cursor.executemany(query_insert, pars)
cursor.commit()
cursor.close()