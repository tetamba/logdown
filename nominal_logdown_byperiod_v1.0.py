# this script is use to get logdown data from nominal 1 database
# from table tb_alarm_jarkom where column offline_at and stop_at for 1 year period 
# only running this script for 1 time only

import logging
import mysql.connector
import datetime
import time

# Create and configure logger
str_today = datetime.datetime.now().strftime("%Y-%m-%d")
log_file = "nominal_logdown_byperiod_v1.0_" + str_today + ".log"
logging.basicConfig(filename=log_file,
                    format='%(asctime)s %(message)s',
                    filemode='a')

# Running app
print("-- Fetch logdown data from database Nominal 1  v1.1--")                    
print("Running app with log: " + log_file)

# Creating an object
logger = logging.getLogger()

# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)

# Creating database connection

# connection1 = mysql.connector.connect(host='172.18.65.122',
#                                      database='wanvolution_dev_backup',
#                                      user='inms2',
#                                      password='inms2-P@ssw0rd')

# connection to nominal 1
connection1 = mysql.connector.connect(host='localhost',
                                        database='nominal1',
                                        user='root',
                                        password='')
# logger.debug("Opened MySQL connection1 to db 'recon'")

# connection to nominal 2
connection2 = mysql.connector.connect(host='localhost',
                                        database='recon',
                                        user='root',
                                        password='')
# logger.debug("Opened MySQL connection2 to db 'recon2'")

# set period 
start_period = datetime.datetime(2022, 1, 1).date()
end_period = datetime.datetime(2022, 12, 1).date()
print("Logdown period %s to %s" %(start_period, end_period))
logger.info("Fetching data from tb_alarm_jarkom periode %s to %s" %(start_period, end_period))

time_start = time.time()
row_counts = 0


for i in range(1, 12):
    if i < 12:
        end_period = start_period.replace(month=i+1)
    else:
        end_period = datetime.datetime(2023, 1, 1).date()
    # print(start_period, end_period)
    
    sql_select = """
        SELECT * FROM tb_alarm_jarkom 
        WHERE (offline_at >= %s AND offline_at < %s AND stop_at IS NOT NULL) OR (stop_at >= %s AND stop_at < %s AND stop_at IS NOT NULL)
        """
    record_select = (start_period, end_period, start_period, end_period)
    cursor = connection1.cursor()
    cursor.execute(sql_select, record_select)
    # get all records
    records = cursor.fetchall()
    row_count = cursor.rowcount
    row_counts = row_counts + row_count

    logger.debug("Query: " + cursor.statement)
    logger.debug("Total number of rows in table: %d" %(row_count))

    sql_insert = """
        INSERT IGNORE INTO tb_alarm_jarkom_copy
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
        """

    cursor = connection2.cursor()
    cursor.executemany(sql_insert, records)
    connection2.commit()
    logger.info("Record inserted successfully into tb_alarm_jarkom_copy table. %d rows affected" %(cursor._rowcount))
    cursor.close
    start_period = end_period

connection1.close()
connection2.close()

duration = time.time() - time_start
logger.debug("Total number of rows: %d" %(row_counts))
logger.info("Execution time %d second(s)" %(duration))
print("Finished with %d second(s) execution time" %(duration) )
    
