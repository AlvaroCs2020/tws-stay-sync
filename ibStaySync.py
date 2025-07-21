import threading
import pandas as pd
from IbDbFetcher import IbDbDataFetcher
from TradingApp import TradingApp
from WatchDog import Watchdog
from dotenv import load_dotenv
import os
from WatchDog import raise_in_main_thread, WatchdogTimeout
import subprocess
from TelegramBot import TelegramBot
from SupaBase import SupaBase
from datetime import time
import time
from time import sleep
from datetime import timezone
from datetime import datetime
import signal
DB_LIMIT = 3 #Esto lo va a pisar el .env

db_config = {
    "dbname": "abbyTrader",
    "user": "postgres",
    "password": "123456789",
    "host": "200.58.123.179",
    "port": 6432
}

def sync(watchdog):
    load_dotenv()
    supa_base_processor = SupaBase()
    # Cargar variables desde el archivo .env
    DB_LIMIT = os.getenv("DB_LIMIT")
    # Obtener la variable como string
    valores_str = os.getenv("SYMBOLS", "")
    TelegramBot.send_message(f"*[WARN]* Se inicio el proceso de sync para los simbolos: *{valores_str}* en caso de ser este el ultimo mensaje *TODO OK*")
    # Convertir la cadena a lista de enteros
    SYMBOL_IDS = [int(v.strip()) for v in valores_str.split(",") if v.strip()]

    app = None  # Inicializamos app para que exista incluso si hay error antes
    try:
        contract_info_by_id = {}

        fetcher = IbDbDataFetcher(db_config)
        for sym_id in SYMBOL_IDS:
            symbol_data_supa_base = supa_base_processor.fetch_symbol_data(str(sym_id))
            symbol_data = fetcher.fetch_symbol_data(str(sym_id))
            contract_info_by_id[sym_id] = {
                'symbol': str(symbol_data.at[0, 'SYMBOL']),
                'sec_type': str(symbol_data.at[0, 'SEC_TYPE']),
                'exchange': str(symbol_data.at[0, 'EXCHANGE']),
                'currency': str(symbol_data.at[0, 'CURRENCY']),
                'symbol_name': str(symbol_data.at[0, 'SYMBOL_NAME']),
                'close_hour': str(symbol_data_supa_base.at[0, 'close_hour'])
            }
            print("SYMBOL: " + str(symbol_data.at[0, 'SYMBOL_NAME']))
        fetcher.close()

        app = TradingApp(contract_info_by_id)
        app.connect("127.0.0.1", 7497, clientId=5)
        threading.Thread(target=app.run, daemon=True).start()
        time.sleep(3)

        if not app.isConnected():
            print("connection failed")
            app.disconnect()
            return
        print("connected")

        while app.isConnected():
            fetcher = IbDbDataFetcher(db_config)
            data_to_process_from_db = pd.DataFrame()
            created_data_list = []
            for sym_id in SYMBOL_IDS:
                df_temp = fetcher.fetch_created_data(symbol_id=sym_id,limit=DB_LIMIT)

                created_data_list.append(df_temp)

            data_to_process_from_db = pd.concat(created_data_list, ignore_index=True)
            fetcher.close()

            results = []
            start_time = time.time()

            if len(data_to_process_from_db) == 0:
                continue

            for index, row in data_to_process_from_db.iterrows():
                if not app.isConnected():
                    return

                try:
                    date_from = row['DATE_FROM'].strftime('%Y%m%d-%H:%M:%S')
                    date_to   = row['DATE_TO'].strftime('%Y%m%d-%H:%M:%S')
                    symbol_id = int(row['SYMBOL_ID'])
                    df_filtered_1min = pd.DataFrame()  # vaciamos antes por las dudas

                    df_filtered_1min = app.get_ticks_per_bar(date_from, date_to, symbol_id=symbol_id)
                    # aca podria, instaciar un sub process e ir cargando la liquidez por segundo

                    if not app.req_made:
                        raise KeyError("No se recibió respuesta válida de TWS")
                    elif app.req_made and app.last_tick_count == 0:
                        raise KeyError(f"No estan llegando ticks {date_from}")
                    sum_ask = df_filtered_1min['SizeAsk'].sum()

                    if app.req_made and sum_ask == 0 and not TradingApp.market_is_closing(row['DATE_FROM'], int(symbol_id)):
                        raise KeyError(f"registro sum 0 {len(df_filtered_1min)}")
                    if sum_ask == 0 and TradingApp.market_is_closing(row['DATE_FROM'], int(symbol_id)):
                        print("[INFO] se guardara un registro sin ticks, ya que es un horario de cierre de mercado")
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
                except WatchdogTimeout:
                    print(f"[ERROR] Ladro el perro")
                    TelegramBot.send_message(f"*[WARN]* Se desconecto TWS para los simbolos: *{valores_str}* *WatchDog*")
                    app.disconnect()
                except Exception as e:
                    print(f"[ERROR] Fallo inesperado en el procesamiento del ID {row['ID']}: {e}")
                    data_to_process_from_db = data_to_process_from_db.drop(index)
                    results.append(str(row['ID']))

                print(f"Progreso {index + 1}/{DB_LIMIT} - ID: {row['ID']} - SYMBOL {row['SYMBOL_ID']} - Fecha: {row['DATE_FROM']}")

            time.sleep(0.5)
            fetcher = IbDbDataFetcher(db_config)
            db_start = time.time()
            fetcher.update_data(data_to_process_from_db)
            fetcher.close()
            db_end = time.time()

            print("Tiempo en update DB:", db_end - db_start)
            print("No se pudieron obtener:", len(results), results)
            print("Tiempo total del ciclo:", time.time() - start_time)
            ##ACA SE Deberia reiniciar el timer del watch dog
            watchdog.reset()
        app.disconnect()

    except KeyboardInterrupt:
        print("\n[INFO] Interrupción por teclado. Cerrando conexión.")
        TelegramBot.send_message(f"*[WARN]* Se desconecto TWS para los simbolos: *{valores_str}* *KeyboardInterrupt*")
        app.disconnect()
        watchdog.stop()
        exit(-2)
    except WatchdogTimeout:
        print(f"[ERROR] Ladro el perro:")
        watchdog.stop()
        app.disconnect()
        TelegramBot.send_message(f"*[WARN]* Se desconecto TWS para los simbolos: *{valores_str}* *WatchdogTimeout*")
        return
    except Exception as e:
        print(f"[ERROR] Excepción general: {e}")
        watchdog.stop()
        app.disconnect()
        TelegramBot.send_message(f"*[WARN]* Se desconecto TWS para los simbolos: *{valores_str}* *{e}*")

    finally:
        watchdog.stop()
        if app and app.isConnected():
            app.disconnect()
def main():
    process = None

    def kill_process():
        print("[WARN] Se decidio cerrar TWS")

        send_command_path = r"C:\IBC\SendCommand.bat"
        working_dir = r"C:\IBC"

        subprocess.run([send_command_path, "STOP"], cwd=working_dir, shell=True)
        time.sleep(5)
    def on_timeout(): ##CallBack del watch dog
        print("[WATCHDOG] Se colgó sync. Matamos el proceso.")
        raise_in_main_thread(WatchdogTimeout)
        valores_str = os.getenv("SYMBOLS", "")
        TelegramBot.send_message(f"*[WARN]* se ejecuto el watch dog para la instancia de TWS que se encarga de los simbolos: *{valores_str}*")

    # Arrancamos el watch dog
    watchdog = Watchdog(timeout=600, callback=on_timeout) #15min

    try:
        while True:
            try:
                watchdog.start()
                print("Intentamos conectarnos")
                # Iniciar .bat en nuevo grupo de procesos
                process = subprocess.Popen(
                    ["cmd.exe", "/c", "C:\\IBC\\StartTWS.bat"],
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP
                )

                time.sleep(60)  # o reemplazá por sync()
                sync(watchdog)

            except Exception as e:
                print(f"[ERROR] Fallo durante la ejecución: {e}")
            finally:
                kill_process()

            print("Reintentamos en 5 segundos...\n")
            time.sleep(5)

    except KeyboardInterrupt:
        print("\n[INTERRUPT] Ctrl+C recibido. Cerrando todo...")
        kill_process()

if __name__ == "__main__":
    main()