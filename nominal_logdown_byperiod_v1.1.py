# this script is use to get logdown data from nominal 1 database
# from table tb_alarm_jarkom where column offline_at and stop_at for the last 1 month period 
# only running this script for early week monthly

import logging
import mysql.connector
import datetime
import time

try:
    # Get date now 
    today = datetime.date.today()
    first = today.replace(day=1)
    last_month = first - datetime.timedelta(days=1)
    first_month = last_month.replace(day=1)
    
    # Create and configure logger
    str_today = datetime.datetime.now().strftime("%Y-%m-%d")
    log_file = "nominal_logdown_" + str_today + ".log"
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

    start_period = first_month
    end_period = first
    start_period = "2022-10-01"
    end_period = "2022-11-01"

    print("Logdown period %s to %s" %(start_period, end_period))
    logger.info("Fetching data from tb_alarm_jarkom periode %s to %s" %(start_period, end_period))
   
    time_start = time.time()
    row_count = 0

    try:
        sql_select = """
            SELECT * FROM tb_alarm_jarkom 
            WHERE (offline_at >= %s AND offline_at < %s AND stop_at IS NOT NULL) OR (stop_at >= %s AND stop_at < %s AND stop_at IS NOT NULL)
            """
        record_select = (start_period, end_period, start_period, end_period)
        cursor1 = connection1.cursor()
        cursor1.execute(sql_select, record_select)
        # get all records
        records = cursor1.fetchall()
        row_count = cursor1.rowcount

        logger.debug("Query: " + cursor1.statement)
        logger.debug("Total number of rows in table: %d" %(row_count))

        record_insert_list = []

        for row in records:
            record_insert = (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],row[13],row[14],row[15],row[16],row[17],row[18],row[19],row[20],row[21],row[22],row[23],row[24])
            record_insert_list.append(record_insert)
    except mysql.connector.errors.Error as e:
        logger.error("Query failed", e)
    finally:
        cursor1.close()
        # logger.info("Cursor 1 is closed")

    try:
        sql_insert = """
            INSERT IGNORE INTO tb_alarm_jarkom_copy
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
            """
        # print(record_insert_list)
        cursor2 = connection2.cursor()
        cursor2.executemany(sql_insert, record_insert_list)
        connection2.commit()
        logger.info("Record inserted successfully into tb_alarm_jarkom_copy table. %d rows affected" %(cursor2._rowcount))
    except mysql.connector.errors.Error as e:
        logger.error("Query failed", e)
    finally:
        cursor2.close()
        # logger.info("Cursor 2 is closed")

    duration = time.time() - time_start
    logger.info("Execution time %d second(s)" %(duration))
    print("Finished with %d second(s) execution time" %(duration) )
     
except mysql.connector.Error as e:
    logger.error("MySQL error", e)

finally:
    if connection1.is_connected():
        connection1.close()
        # logger.debug("MySQL connection1 is closed")

    if connection2.is_connected():
        connection2.close()
        # logger.debug("MySQL connection2 is closed")

    