import time
import threading
from typing import AnyStr

import pandas as pd
import random
from datetime import datetime, timezone
from IbDbFetcher import IbDbDataFetcher
from TradingApp import *
STACK_SIZE = 5
DB_LIMIT = 5
# DB_LIMIT = 100
db_config = {
    "dbname": "abbyTrader",
    "user": "postgres",
    "password": "123456789",
    "host": "200.58.123.179",
    "port": 6432
}
def sync():
    try:
        app = TradingApp()
        app.connect("127.0.0.1", 7497, clientId=5)
        threading.Thread(target=app.run, daemon=True).start()
        time.sleep(2)
        if app.isConnected():
            print("connected")
        else:
            app.disconnect()
            print("connection failed")
            return
        fetcher = IbDbDataFetcher(db_config)

        #fetcher.close()
        #data_slices = [data_to_process_from_db[i:i+STACK_SIZE] for i in range(0, len(data_to_process_from_db), STACK_SIZE)]
        while app.isConnected():
            results = []
            start_time = time.time()
            count = 0
            # for index, row in data_to_process_from_db.iterrows():
            data_to_process_from_db = fetcher.fetch_created_data(limit=DB_LIMIT)
            for index, row in data_to_process_from_db.iterrows():
                if not app.isConnected():
                    return
                date_from = row['DATE_FROM'].strftime('%Y%m%d-%H:%M:%S')
                date_to = row['DATE_TO'].strftime('%Y%m%d-%H:%M:%S')
                df_filtered_1min = app.get_ticks_per_bar(date_from, date_to)
                try:
                    sum_ask = df_filtered_1min['SizeAsk'].sum()
                    sum_bid = df_filtered_1min['SizeBid'].sum()
                    difference = sum_bid - sum_ask
                    date_from_db_format = pd.to_datetime(date_from, utc=True)
                    date_to_db_format = pd.to_datetime(date_to, utc=True)
                    diff_str = app.convert_values_to_str(sum_ask, sum_bid)
                    count_tick = len(df_filtered_1min)
                    updated_at = datetime.now(timezone.utc)
                    nw_day = date_from_db_format.weekday() >= 5
                    if sum_ask == 0 and app.last_tick_count == 0:  # si me vinieron ticks pero no se termino sumando nada, seguramente no hubo ticks en esa vela
                        raise KeyError
                    if sum_ask == 0 and app.last_tick_count != 0:
                        print("No hay ticks en este intervalo, id: " + str(row['ID']) + ", inicio " + str(row['DATE_FROM']))
                    data_to_process_from_db.loc[index, 'SUM_ASK']        = sum_ask
                    data_to_process_from_db.loc[index, 'SUM_BID']        = sum_bid
                    data_to_process_from_db.loc[index, 'DIFFERENCE']     = difference
                    data_to_process_from_db.loc[index, 'SUM_ASK_STR']    = TradingApp.format_int_to_string(sum_ask)
                    data_to_process_from_db.loc[index, 'SUM_BID_STR']    = TradingApp.format_int_to_string(sum_bid)
                    data_to_process_from_db.loc[index, 'DIFFERENCE_STR'] = TradingApp.format_int_to_string(difference)
                    data_to_process_from_db.loc[index, 'COUNT_TICK']     = count_tick
                    data_to_process_from_db.loc[index, 'STATUS']         = "SUCCESS"
                    data_to_process_from_db.loc[index, 'UPDATED_AT']     = updated_at
                    data_to_process_from_db.loc[index, 'DIFF_LEVEL_ENUM']= diff_str
                    data_to_process_from_db.loc[index, 'RETRY_COUNT']    = 0
                    data_to_process_from_db.loc[index, 'DOW']            = date_from_db_format.weekday()
                    data_to_process_from_db.loc[index, 'NW_DAY']         = nw_day

                except KeyError:
                    print(f"No se guardo este id: {row['ID']} first tws request {app.last_tick_count} sum ask {sum_ask} ticks {len(df_filtered_1min)}")
                    data_to_process_from_db = row.drop(index)
                    results.append(str(row['ID']))
                count = (count + 1) % (STACK_SIZE+1)
                print(f"progreso {count}/{STACK_SIZE} id {row['ID']} fecha : {row['DATE_FROM']}")
            start_time_db = time.time()
            #fetcher = IbDbDataFetcher(db_config)
            #time.sleep(2)
            fetcher.update_data(data_to_process_from_db)
            end_time_db = time.time()
            print("tardamos  en el update:"+str(end_time_db - start_time_db))
            df_results = pd.DataFrame(results)
            end_time = time.time()
            print("No se puedieron obtener: " + str(len(results)))
            print("tardamos  en hacer todo:"+str(end_time - start_time))
            print(results)
        app.disconnect()
    except KeyboardInterrupt:
        app.disconnect()
        exit(-2)

