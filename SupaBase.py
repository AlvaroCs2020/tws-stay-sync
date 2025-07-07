import pandas as pd
import time
from dotenv import load_dotenv
import os
import psycopg2
from psycopg2.extras import execute_values
from supabase import create_client, Client
from datetime import datetime

class SupaBase:
    def __init__(self):
        self.data_to_save = []
        self.df_empty = pd.DataFrame(columns=[
            'Time', 'TickAttriBidAsk', 'AskPastHigh', 'PriceBid',
            'PriceAsk', 'SizeBid', 'SizeAsk', 'TimeFormatted'
        ])
        load_dotenv()
        self.db_url = os.getenv('DB_URL')
        self.conn = psycopg2.connect(
            self.db_url,
        )
        self.max_retries = 5
        self.retry_wait = 2
        self.curr = self.conn.cursor()
    def __connect(self):
        try:
            self.conn = psycopg2.connect(self.db_url,)
            self.conn.autocommit = False
            print("[INFO] Conexión a la base de datos establecida.")
        except psycopg2.OperationalError as e:
            print(f"[ERROR] No se pudo conectar a la base de datos: {e}")
            self.conn = None

    def __ensure_connection(self):
        if self.conn is None or self.conn.closed:
            print("[WARN] Conexión cerrada o inválida, reintentando...")
            for attempt in range(self.max_retries):
                try:
                    self.__connect()
                    if self.conn and not self.conn.closed:
                        print("[INFO] Reconexión exitosa.")
                        return
                except Exception as e:
                    print(f"[ERROR] Reintento {attempt+1} fallido: {e}")
                    time.sleep(self.retry_wait)
            raise Exception("No se pudo restablecer la conexión a la base de datos.")
    def __insert_new_values(self):
        sql = """
            INSERT INTO "LIQUIDEZTEST" (
                date_id, symbol_id, updated_at, sum_ask, sum_bid,
                difference, count_tick, price_bid, price_ask, boolean
            ) VALUES %s
        """
        execute_values(self.curr, sql,self.data_to_save)
        self.conn.commit()
    def receive_and_process_data(self, data : pd.DataFrame, symbol_id : int):
        chunks_by_second = data.groupby('Time') #error handling
        for second, chunk in chunks_by_second:
            sum_ask = chunk['SizeAsk'].sum()
            sum_bid = chunk['SizeBid'].sum()
            difference = sum_bid - sum_ask
            count_tick = len(chunk)
            last = chunk.loc[chunk.index[-1]]
            price_bid = last['PriceBid'] #Si o si el ultimo precio? un promedio, algo de eso, pensar!!
            price_ask = last['PriceAsk']
            date_id =  pd.to_datetime(chunk['Time'].max(), unit='s', utc=True)

            new_row = {"date_id"    :date_id,
                       "symbol_id"  :int(symbol_id),
                       "sum_ask"    :int(sum_ask),
                       "sum_bid"    :int(sum_bid),
                       "difference" :int(difference),
                       "count_tick" :int(count_tick),
                       "price_bid"  :float(price_bid),
                       "price_ask"  :float(price_ask)} #falta algo?

            self.data_to_save.append(new_row)


    def save_data_to_supabase(self):
        if len(self.data_to_save) == 0:
            return
        print("++++++++++++++++++++++++++++++++++++++++++++")
        print(f"TOTAL AL GUARDAR {len(self.data_to_save)}")
        print("++++++++++++++++++++++++++++++++++++++++++++")
        self.__ensure_connection()
        self.__insert_new_values()
        # for new_row in self.data_to_save:
        #     text = f"""
        #     # date_id    :{new_row["DATE_ID"]}
        #     # symbol_id  :{new_row["SYMBOL_ID"]}
        #     # sum_ask    :{new_row["SUM_ASK"]}
        #     # sum_bid    :{new_row["SUM_BID"]}
        #     # difference :{new_row["DIFFERENCE"]}
        #     # count_tick :{new_row["COUNT_TICK"]}
        #     # price_bid  :{new_row["PRICE_BID"]}
        #     # price_ask  :{new_row["PRICE_ASK"]}              """
        #     print(text)
        #     print("++++++++++++++++++++++++++++++++++++++++++++")

        self.data_to_save = []
    def __del__(self):
        print("MURIO ESTA INSTANCIA")