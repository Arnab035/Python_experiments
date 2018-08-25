# modify the database connection

from bittrex_websocket import BittrexSocket
import time
import datetime
import ccxt

import MySQLdb as bitdb

# print (ccxt.exchanges)

bittrex_exchange = ccxt.bittrex()

def insert_data(time, _open, 
                 _close, low, high, volume,
                 market, cursor):
    sql = "INSERT INTO oneminmarket (time_of_entry, market, \
           opening, closing, low, high, volume) \
           VALUES (%s, %s, %s, %s, %s, %s, %s)"
    values = (time, market, _open, _close,
              low, high, volume)
    cursor.execute(sql, values)
    # cursor.close()

   
if __name__ == "__main__":
    if bittrex_exchange.has['fetchOHLCV']:
        db = bitdb.connect(
                host='mysql****.*******.ap-south-1.rds.amazonaws.com',
                port=3306,
                user='******',
                passwd='*******',
                db='bittrex_exchange'
                )
        markets = bittrex_exchange.load_markets()
        cursor = db.cursor()

        # part 1 working
        for symbol in markets:
            time.sleep(bittrex_exchange.rateLimit / 1000)
            print symbol
            data = bittrex_exchange.fetch_ohlcv(symbol, '1m')
                # only the last and most recent element of the array  
                # for i in range(0, len(data)):
            insert_data(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(data[-1][0]/1000))
            , data[-1][1], 
            data[-1][2], data[-1][3], data[-1][4], data[-1][5],
            symbol, cursor)
            db.commit()

        cursor.close()
        db.close()
