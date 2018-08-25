# database connection to be changed

import time
import datetime

import MySQLdb as bitdb

# print (ccxt.exchanges)

def store_processed_data(cursor, table, seconds):
    sql = "SELECT \
           FLOOR(MIN(unix_timestamp(time_of_entry))/" +str(seconds)+ ") *" +str(seconds)+" AS time_of_entry, \
           market, \
           SUBSTRING_INDEX(MIN(CONCAT(unix_timestamp(time_of_entry), '_', opening)), '_',-1) AS opening, \
           SUBSTRING_INDEX(MAX(CONCAT(unix_timestamp(time_of_entry), '_', closing)), '_',-1) \
           AS closing, \
           MIN(low) AS low, \
           MAX(high) AS high, \
           SUM(volume) AS volume \
           FROM oneminmarket \
           GROUP BY FLOOR(unix_timestamp(time_of_entry)/" + str(seconds) + "), market \
           ORDER BY time_of_entry"

    sql1 = "INSERT INTO " + table + " (time_of_entry, market, \
            opening, closing, low, high, volume) \
            VALUES (%s, %s, %s, %s, %s, %s, %s)"

    cursor.execute(sql)
    for row in cursor.fetchall():
        # print datetime.datetime.strptime(str(row[0]), "%Y%m%d%H%M%S")
        values = (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row[0])), row[1], row[2], row[3], row[4], row[5], row[6])
        cursor.execute(sql1, values)

   
if __name__ == "__main__":
    db = bitdb.connect(
            host='mysql*****.*****.ap-south-1.rds.amazonaws.com',
            port=3306,
            user='****',
            passwd='*******',
            db='bittrex_exchange'
            )
    cursor = db.cursor()

    # part 2 working

    store_processed_data(cursor, 'fiveminmarket', 300)
    db.commit()

    store_processed_data(cursor, 'fifteenminmarket', 900)
    db.commit()

    store_processed_data(cursor, 'thirtyminmarket', 1800)
    db.commit()

    store_processed_data(cursor, 'onehourmarket', 3600)
    db.commit()

    print "Successfully updated all 5min, 15min, 30min and 1hr entries"
    cursor.close()
    db.close()
