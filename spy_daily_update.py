import pandas as pd

import pandas_datareader.data as web
import sqlite3
import datetime

conn = sqlite3.connect("daily.db")

def updateDatabase_daily():
    try:
        #check the most recent date(primary key) in database 
        start_dt = pd.to_datetime(pd.read_sql('select max(dt) from price_daily', conn).values[0,0]).date()
    except:
        start_dt = datetime.date(2005,1,1)

    today = datetime.datetime.today()
    if today > datetime.datetime(today.year, today.month, today.day, 16, 0, 0):
        # check wether after market close
        end_dt = today
    else:
        end_dt = today - datetime.timedelta(days=1)
        
    end_dt = end_dt.date()

    if (end_dt - start_dt).days < 1:
        print('database is most updated')

    else:    
        print('start update data during:', start_dt, end_dt)
        try:
            df = web.DataReader(['^VIX', 'TLT', '^GSPC'], 'yahoo', start_dt, end_dt)['Adj Close']
            df.index.name = 'dt'
            df.columns.name = None

            print('save data to database')
            df.to_sql('price_daily', conn, if_exists='append')
        except:
            print('No data is updated')
            
            
if __name__ == "__main__":
    updateDatabase_daily()
