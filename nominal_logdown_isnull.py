# this script is use to get logdown data from nominal 1 database
# from table tb_alarm_jarkom where column stop_at is NULL for the last 1 year period 
# only running this script for first time 

import logging
import mysql.connector
import datetime
import time

try:
    # Get date now 
    today = datetime.datetime.now().strftime("%Y-%m-%d")

    # Create and configure logger
    log_file = "nominal_logdown_isnull_" + today + ".log"
    logging.basicConfig(filename=log_file,
                        format='%(asctime)s %(message)s',
                        filemode='a')

    # Running app
    print("-- Fetch logdown data from database Nominal 1 with stop_at is null --")                    
    print("Running app with log: " + log_file)

    # Creating an object
    logger = logging.getLogger()
    
    # Setting the threshold of logger to DEBUG
    logger.setLevel(logging.DEBUG)

    # Creating database connection

    connection1 = mysql.connector.connect(host='172.18.65.122',
                                         database='wansolution_dev_backup',
                                         user='inms2',
                                         password='inms2-P@ssw0rd')
    
    start_period = "2022-01-01"
    end_period = "2023-01-01"

    print("Logdown period %s to %s" %(start_period, end_period))
    logger.info("Fetching data from tb_alarm_jarkom periode %s to %s" %(start_period, end_period))
   
    time_start = time.time()
    row_count = 0

    try:
        sql_select = """
            SELECT * FROM tb_alarm_jarkom 
            WHERE (offline_at >= %s AND offline_at < %s) AND stop_at IS NULL
            """
        record_select = (start_period, end_period)
        cursor1 = connection1.cursor()
        cursor1.execute(sql_select, record_select)
        # get all records
        records = cursor1.fetchall()
        row_count = cursor1.rowcount

        logger.debug("Query: " + cursor1.statement)
        logger.debug("Total number of rows in table: %d" %(row_count))

    except mysql.connector.errors.Error as e:
        logger.error("Query failed", e)
    finally:
        cursor1.close()
        # logger.info("Cursor 1 is closed")

    duration = time.time() - time_start
    logger.info("Execution time %d second(s)" %(duration))
    print("Finished with %d second(s) execution time" %(duration) )

except mysql.connector.Error as e:
    logger.error("MySQL error", e)

finally:
    if connection1.is_connected():
        connection1.close()
        # logger.debug("MySQL connection1 is closed")


    
