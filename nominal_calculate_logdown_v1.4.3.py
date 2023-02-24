# this script is use to calculate logdown data from recon database
# from table tb_alarm_jarkom_copy with remedy ticket data 
# only running this script for early week monthly
# output: availability rekap harian

import logging
import mysql.connector
import datetime
import time
import pandas as pd

try:
    # Get today 
    today = datetime.date.today()
    first = today.replace(day=1)
    false_alarm = 300
    last_month = first - datetime.timedelta(days=1)
    first_month = last_month.replace(day=1)

    # Create and configure logger
    str_today = datetime.datetime.now().strftime("%Y-%m-%d")
    log_file = "nominal_calculate_logdown_v1.4.3_" + str_today + ".log"
    logging.basicConfig(filename=log_file,
                        format='%(asctime)s %(message)s',
                        filemode='a')

    # Running app
    print("-- Calculate logdown downtime from database recon v1.4 --")                    
    print("Running app with log: " + log_file)

    # Creating an object
    logger = logging.getLogger()
    
    # Setting the threshold of logger to DEBUG
    logger.setLevel(logging.DEBUG)

    # Creating database connection
    
    # connection = mysql.connector.connect(host='localhost',
    #                                      database='dbrecon_20220217',
    #                                      user='nominalf2',
    #                                      password='P@ssw0rdN0m1n4l')
    connection = mysql.connector.connect(host='localhost',
                                         database='dbrecon_20220217',
                                         user='root',
                                         password='')
    start_period = first_month
    end_period = first
    dt_start_period = datetime.datetime.combine(start_period, datetime.time(0, 0))
    dt_end_period = datetime.datetime.combine(end_period, datetime.time(0, 0))
    days = dt_end_period - dt_start_period
    logger.info("Running script v1.4")
    print("Logdown period %s to %s" %(start_period, end_period))
    logger.info("Fetching data from tb_alarm_jarkom_copy periode %s to %s" %(start_period, end_period))
   
    time_start = time.time()

    try:
        # get record alarm stop_at is not null
        sql_select = """
            SELECT * FROM tb_alarm_jarkom_copy
            WHERE id_alarm_type IN (0,1,11,12,13,14,15,16,38,99) 
                AND ((offline_at >= %s AND offline_at < %s AND stop_at IS NOT NULL) 
                    OR (stop_at >= %s AND stop_at < %s AND stop_at IS NOT NULL))
            """
        record_select = (start_period, end_period, start_period, end_period)
        cursor = connection.cursor()
        cursor.execute(sql_select, record_select)
        
        jarkom_time_offline = {}
        jarkom_offline_list = {}
        jarkom_daily_list = []
 
        # get all records
        records = cursor.fetchall()
        row_count = cursor.rowcount
        for row in records:
            # logger.debug("row[1]:%s, row[5]:%s, row[6]:%s, row[7]:%s, row[8]:%s" %(row[1],row[5],row[6],row[7],row[8]))
            # print("start_at:%s,offline_at:%s, stop_at:%s" %(row[6],row[7],row[8]))
            id_jarkom = row[1]
            kode_jarkom = row[2]
            id_remote = row[3]
            kode_provider = row[4]
            ip_address = row[18]
            offline_at = row[7]
            stop_at = row[8]
            # filter NULL and cut off date range
            if offline_at is None:
                offline_at = dt_start_period
            if offline_at < dt_start_period:
                offline_at = dt_start_period
            if stop_at > dt_end_period:
                stop_at = dt_end_period
            
            # print("id_jarkom:%s,offline_at:%s, stop_at:%s" %(id_jarkom,offline_at,stop_at))
            
            # split duration offline daily
            start = offline_at
            end = stop_at
            dates = [datetime.datetime.strptime(str(date), "%Y-%m-%d %H:%M:%S") for date in pd.date_range(start.date() + datetime.timedelta(1), end.date())] + [start, end]
            dates = sorted(dates)

            jarkom_daily = []
            for i in range(len(dates)-1):
                jarkom_daily_list.append((id_jarkom, dates[i], dates[i+1]))
                # print(id_jarkom,dates[i],dates[i+1])
                # print(jarkom_daily)
        df = pd.DataFrame(jarkom_daily_list, columns =['id_jarkom', 'start', 'end'])
        # print(df)
        # df.to_csv("jarkom_daily_list.csv", sep=',', encoding='utf-8')

    except mysql.connector.errors.Error as e:
        logger.error("Query failed", e)
    finally:
        cursor.close()

    duration = time.time() - time_start
    logger.info("Execution time %d second(s)" %(duration))
    print("Finished with %d second(s) execution time" %(duration) )
    
except mysql.connector.Error as e:
    logger.error("MySQL error", e)

finally:
    if connection.is_connected():
        connection.close()


    
