# this script is use to calculate logdown data from recon database
# from table tb_alarm_jarkom_copy with remedy ticket data 
# only running this script for early week monthly
# output: availability rekap bulanan

import logging
import mysql.connector
import datetime
import time

try:
    # Get today 
    today = datetime.date.today()
    first = today.replace(day=1)
    false_alarm = 300
    last_month = first - datetime.timedelta(days=1)
    first_month = last_month.replace(day=1)
    # dt_start_period = datetime.datetime.strptime(last_month, "%Y-%m-%d")
    # dt_end_period = datetime.datetime.strptime(first_month, "%Y-%m-%d")  

    # Create and configure logger
    str_today = datetime.datetime.now().strftime("%Y-%m-%d")
    log_file = "nominal_calculate_logdown_" + str_today + ".log"
    logging.basicConfig(filename=log_file,
                        format='%(asctime)s %(message)s',
                        filemode='a')

    # Running app
    print("-- Calculate logdown downtime from database recon v1.4.2 --")                    
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
    # start_period = "2022-10-01"
    # end_period = "2022-11-01"
    # start_period = datetime.datetime.strptime(start_period, "%Y-%m-%d")
    # end_period = datetime.datetime.strptime(end_period, "%Y-%m-%d")
    dt_start_period = datetime.datetime.combine(start_period, datetime.time(0, 0))
    dt_end_period = datetime.datetime.combine(end_period, datetime.time(0, 0))
    days = dt_end_period - dt_start_period
    # print("dt start period: ", dt_start_period)
    # print("dt end period", dt_end_period)
    # print("days:", dt_end_period - dt_start_period, "seconds:", days.total_seconds(), "av.:", av)
    # exit(0)
    logger.info("Running script v1.4.2")
    print("Logdown period %s to %s" %(start_period, end_period))
    logger.info("Fetching data from tb_alarm_jarkom_copy periode %s to %s" %(start_period, end_period))
   
    time_start = time.time()

    try:
        # get record alarm stop_at is not null
        sql_select = """
            SELECT c.id,c.id_jarkom,c.kode_jarkom,c.id_remote,c.kode_provider,c.ip_address,c.id_alarm_type,c.start_at,c.offline_at,c.stop_at,j.kode_jenis_jarkom,r.ip_lan 
            FROM tb_alarm_jarkom_copy c
            LEFT JOIN tb_jarkom j ON j.id=c.id_jarkom
            LEFT JOIN tb_remote r ON r.id_remote=c.id_remote
            WHERE c.id_alarm_type IN (0,1,11,12,13,14,15,16,38,99)
                AND ((c.offline_at >= %s AND c.offline_at < %s AND c.stop_at IS NOT NULL) 
                OR (c.stop_at >= %s AND c.stop_at < %s AND c.stop_at IS NOT NULL))
            """
            # AND id_jarkom IN (7815,14102,17767,55509)
        record_select = (start_period, end_period, start_period, end_period)
        cursor = connection.cursor()
        cursor.execute(sql_select, record_select)
        
        jarkom_time_offline = {}
        jarkom_offline_list = {}
        # get all records
        records = cursor.fetchall()
        row_count = cursor.rowcount
        # row[0] : id
        # row[1] : id_jarkom
        # row[2] : kode_jarkom
        # row[3] : id_remote
        # row[4] : kode_provider
        # row[5] : ip_address
        # row[6] : id_alarm_type
        # row[7] : start_at
        # row[8] : offline_at
        # row[9] : stop_at
        # row[10] : kode_jenis_jarkom
        # row[11] : ip_lan
        for row in records:
            # logger.debug("row[1]:%s, row[5]:%s, row[6]:%s, row[7]:%s, row[8]:%s" %(row[1],row[5],row[6],row[7],row[8]))
            # print("start_at:%s,offline_at:%s, stop_at:%s" %(row[7],row[8],row[9]))
            offline_at = row[8]
            stop_at = row[9]
            if offline_at is None:
                offline_at = dt_start_period
            if offline_at < dt_start_period:
                offline_at = dt_start_period
            if stop_at > dt_end_period:
                stop_at = dt_end_period

            delta = stop_at - offline_at
            downtime = delta.total_seconds()
            # print("start_at: %s, offline_at: %s, stop_at:%s, diff:%s, downtime:%d" %(row[6],offline_at,stop_at,delta,downtime))
            if downtime > false_alarm:
                sum_downtime = downtime + jarkom_time_offline.get(row[1], 0)
                jarkom_time_offline[row[1]] = sum_downtime
                jarkom_offline_list[row[1]] = (row[10],row[3],row[5],row[4],row[11])
        
        logger.debug("Query: " + cursor.statement)
        logger.debug("Total number of rows in table: %d" %(row_count))
        logger.debug("Total number of id_jarkom : %d without false alarm" %(len(jarkom_time_offline)))

        # get record alarm stop_at is  null
        sql_select = """
            SELECT c.id,c.id_jarkom,c.kode_jarkom,c.id_remote,c.kode_provider,c.ip_address,c.id_alarm_type,c.start_at,c.offline_at,c.stop_at,j.kode_jenis_jarkom,r.ip_lan 
            FROM tb_alarm_jarkom_isnull c
            LEFT JOIN tb_jarkom j ON j.id=c.id_jarkom
            LEFT JOIN tb_remote r ON r.id_remote=c.id_remote
            WHERE stop_at IS NULL AND c.id_alarm_type IN (0,1,11,12,13,14,15,16,38,99)
            """
        # record_select = (end_period, )
        cursor = connection.cursor()
        cursor.execute(sql_select)
        # cursor.execute(sql_select, record_select)
        
        jarkom_isnull_time_offline = {} 
        jarkom_offline_isnull_list = {}
        records = cursor.fetchall()
        row_count = cursor.rowcount
        for row in records:
            # logger.debug("row[1]:%s, row[5]:%s, row[6]:%s, row[7]:%s, row[8]:%s" %(row[1],row[5],row[6],row[7],row[8]))
            # print("start_at:%s,offline_at:%s, stop_at:%s" %(row[6],row[7],row[8]))
            offline_at = row[7]
            stop_at = dt_end_period
            if offline_at < dt_start_period:
                offline_at = dt_start_period
            delta = stop_at - offline_at
            downtime = delta.total_seconds()
            # print("start_at:%s,offline_at: %s, stop_at:%s, diff:%s, downtime:%d" %(row[6],offline_at,stop_at,delta,downtime))
            if downtime > false_alarm:
                jarkom_isnull_time_offline[row[1]] = downtime + jarkom_isnull_time_offline.get(row[1], 0)
                jarkom_offline_isnull_list[row[1]] = (row[10],row[3],row[5],row[4],row[11])

        logger.debug("Query: " + cursor.statement)
        logger.debug("Total number of rows in table: %d" %(row_count))
        logger.debug("Total number of id_jarkom : %d without false alarm" %(len(jarkom_isnull_time_offline)))
        
        record_insert_list = []
        days_second = days.total_seconds()
        av = 100
        for key, value in jarkom_time_offline.items():
            record_field = jarkom_offline_list[key]
            diff = days_second - value
            if diff <= 0:    
                av = 0 
            else:
                av = (diff/days_second) * 100
            av = round(av, 2)
            # print("id_jarkom:%s,ip_address: %s, ip_lan: %s, downtime:%d, days_second:%s, diff:%d, av:%f" %(key,record_field[2],record_field[4],value,days_second,diff,av))
            record_insert = (start_period,key,record_field[0],record_field[1],record_field[2],record_field[4],record_field[3],value,av)
            record_insert_list.append(record_insert)
        for key, value in jarkom_isnull_time_offline.items():
            record_field = jarkom_offline_isnull_list[key]
            diff = days_second - value
            if diff <= 0:    
                av = 0 
            else:
                av = (diff/days_second) * 100
            av = round(av, 2)
            # print("id_jarkom:%s,ip_address: %s, ip_lan: %s, downtime:%d, days_second:%s, diff:%d, av:%f" %(key,record_field[2],record_field[4],value,days_second,diff,av))
            record_insert = (start_period,key,record_field[0],record_field[1],record_field[2],record_field[4],record_field[3],value,av)
            record_insert_list.append(record_insert)

        sql_insert = """
            INSERT IGNORE INTO tb_jarkom_downtime (logdown_period,id_jarkom,kode_jarkom,id_remote,ip_address,ip_lan,kode_provider,downtime,av)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) 
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

    duration = time.time() - time_start
    logger.info("Execution time %d second(s)" %(duration))
    print("Finished with %d second(s) execution time" %(duration) )
    
except mysql.connector.Error as e:
    logger.error("MySQL error", e)

finally:
    if connection.is_connected():
        connection.close()


    
