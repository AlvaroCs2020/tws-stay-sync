import time
import threading
from datetime import datetime, timezone
import pandas as pd

from IbDbFetcher import IbDbDataFetcher
from TradingApp import TradingApp

STACK_SIZE = 5
DB_LIMIT = 5

db_config = {
    "dbname": "abbyTrader",
    "user": "postgres",
    "password": "123456789",
    "host": "200.58.123.179",
    "port": 6432
}

def sync():
    app = TradingApp()
    try:
        app.connect("127.0.0.1", 7497, clientId=5)
        threading.Thread(target=app.run, daemon=True).start()
        time.sleep(2)

        if not app.isConnected():
            print("connection failed")
            app.disconnect()
            return
        print("connected")

        while app.isConnected():
            fetcher = IbDbDataFetcher(db_config)
            data_to_process_from_db = fetcher.fetch_created_data(limit=DB_LIMIT)
            fetcher.close()

            results = []
            start_time = time.time()

            for index, row in data_to_process_from_db.iterrows():
                if not app.isConnected():
                    return

                try:
                    date_from = row['DATE_FROM'].strftime('%Y%m%d-%H:%M:%S')
                    date_to   = row['DATE_TO'].strftime('%Y%m%d-%H:%M:%S')

                    df_filtered_1min = pd.DataFrame()  # vaciamos antes por las dudas
                    df_filtered_1min = app.get_ticks_per_bar(date_from, date_to)

                    if not app.req_made:
                        raise KeyError("No se recibió respuesta válida de TWS")
                    elif app.req_made and app.last_tick_count == 0:
                        raise KeyError(f"No estan llegando ticks {date_from}")
                    sum_ask = df_filtered_1min['SizeAsk'].sum()
                    sum_bid = df_filtered_1min['SizeBid'].sum()
                    difference = sum_bid - sum_ask
                    count_tick = len(df_filtered_1min)

                    diff_str = app.convert_values_to_str(sum_ask, sum_bid)
                    updated_at = datetime.now(timezone.utc)

                    data_to_process_from_db.loc[index, 'SUM_ASK']         = sum_ask
                    data_to_process_from_db.loc[index, 'SUM_BID']         = sum_bid
                    data_to_process_from_db.loc[index, 'DIFFERENCE']      = difference
                    data_to_process_from_db.loc[index, 'SUM_ASK_STR']     = TradingApp.format_int_to_string(sum_ask)
                    data_to_process_from_db.loc[index, 'SUM_BID_STR']     = TradingApp.format_int_to_string(sum_bid)
                    data_to_process_from_db.loc[index, 'DIFFERENCE_STR']  = TradingApp.format_int_to_string(difference)
                    data_to_process_from_db.loc[index, 'COUNT_TICK']      = count_tick
                    data_to_process_from_db.loc[index, 'STATUS']          = "SUCCESS"
                    data_to_process_from_db.loc[index, 'UPDATED_AT']      = updated_at
                    data_to_process_from_db.loc[index, 'DIFF_LEVEL_ENUM'] = diff_str
                    data_to_process_from_db.loc[index, 'RETRY_COUNT']     = 0

                except KeyError as e:
                    print(f"[WARN] {e}, ID: {row['ID']}")
                    data_to_process_from_db = data_to_process_from_db.drop(index)
                    results.append(str(row['ID']))
                except Exception as e:
                    print(f"[ERROR] Fallo inesperado en el procesamiento del ID {row['ID']}: {e}")
                    data_to_process_from_db = data_to_process_from_db.drop(index)
                    results.append(str(row['ID']))

                print(f"Progreso {index + 1}/{len(data_to_process_from_db)} - ID: {row['ID']} - Fecha: {row['DATE_FROM']}")

            time.sleep(0.5)
            fetcher = IbDbDataFetcher(db_config)
            db_start = time.time()
            fetcher.update_data(data_to_process_from_db)
            fetcher.close()
            db_end = time.time()

            print("Tiempo en update DB:", db_end - db_start)
            print("No se pudieron obtener:", len(results), results)
            print("Tiempo total del ciclo:", time.time() - start_time)

        app.disconnect()

    except KeyboardInterrupt:
        print("\n[INFO] Interrupción por teclado. Cerrando conexión.")
        app.disconnect()
        exit(-2)
    except Exception as e:
        print(f"[ERROR] Excepción general: {e}")
        app.disconnect()
