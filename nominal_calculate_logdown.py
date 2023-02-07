# this script is use to calculate logdown data from recon database
# from table tb_alarm_jarkom_copy with remedy ticket data 
# only running this script for early week monthly

import logging
import mysql.connector
import datetime
import time

try:
    # Get date now 
    today = datetime.datetime.now().strftime("%Y-%m-%d")

    # Create and configure logger
    log_file = "nominal_calculate_logdown_" + today + ".log"
    logging.basicConfig(filename=log_file,
                        format='%(asctime)s %(message)s',
                        filemode='a')

    # Running app
    print("-- Calculate logdown downtime from database recon  --")                    
    print("Running app with log: " + log_file)

    # Creating an object
    logger = logging.getLogger()
    
    # Setting the threshold of logger to DEBUG
    logger.setLevel(logging.DEBUG)

    # Creating database connection

    # connection = mysql.connector.connect(host='172.18.65.122',
    #                                      database='wansolution_dev_backup',
    #                                      user='inms2',
    #                                      password='inms2-P@ssw0rd')
    
    # connection to nominal 2
    connection = mysql.connector.connect(host='localhost',
                                         database='recon2',
                                         user='root',
                                         password='')
    # logger.debug("Opened MySQL connection1 to db 'recon'")

    start_period = "2022-11-01"
    end_period = "2022-12-01"

    print("Logdown period %s to %s" %(start_period, end_period))
    logger.info("Fetching data from tb_alarm_jarkom_copy periode %s to %s" %(start_period, end_period))
   
    time_start = time.time()

    try:
        sql_select = """
            SELECT *,TIMESTAMPDIFF(SECOND,offline_at,stop_at) AS downtime FROM tb_alarm_jarkom_copy
            WHERE (offline_at >= %s AND offline_at < %s AND stop_at IS NOT NULL) 
                OR (stop_at >= %s AND stop_at < %s AND stop_at IS NOT NULL)
                AND id_alarm_type IN (0,1,11,12,13,14,15,16,38,99)
            ORDER BY id_jarkom
            """
        record_select = (start_period, end_period, start_period, end_period)
        cursor = connection.cursor()
        cursor.execute(sql_select, record_select)
        
        row_count = 0
        jarkom_time_offline = {}
        # get all records
        records = cursor.fetchall()
        row_count = cursor.rowcount
        for row in records:
            # print("row[1]:",row[1],"- value",row[25])
            if row[25] > 300 :
                jarkom_time_offline[row[1]] = jarkom_time_offline.get(row[1],0) + row[25]
        
        logger.debug("Query: " + cursor.statement)
        logger.debug("Total number of rows in table: %d" %(row_count))
        logger.debug("Total number of id_jarkom : %d without false alarm" %(len(jarkom_time_offline)))

        record_insert_list = []
        for key, value in jarkom_time_offline.items():
            record_insert = (key,start_period,end_period,value)
            record_insert_list.append(record_insert)

        sql_insert = """
            INSERT INTO tb_jarkom_downtime (id_jarkom,start_period,end_period,downtime)
            VALUES (%s, %s, %s, %s) 
            """
        # print(record_insert_list)
        cursor = connection.cursor()
        cursor.executemany(sql_insert, record_insert_list)
        connection.commit()
        logger.info("Record inserted successfully into tb_jarkom_downtime table. %d rows affected" %(cursor._rowcount))

    except mysql.connector.errors.Error as e:
        logger.error("Query failed", e)
    finally:
        cursor.close()
        # logger.info("Cursor is closed")

    duration = time.time() - time_start
    logger.info("Execution time %d second(s)" %(duration))
    print("Finished with %d second(s) execution time" %(duration) )
    
except mysql.connector.Error as e:
    logger.error("MySQL error", e)

finally:
    if connection.is_connected():
        connection.close()
        # logger.debug("MySQL connection1 is closed")


    
