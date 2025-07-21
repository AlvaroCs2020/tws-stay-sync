import pandas as pd
import time
from dotenv import load_dotenv
import os
import psycopg2
import sys
from psycopg2.extras import execute_values
from supabase import create_client, Client
from datetime import datetime
from datetime import time
from time import *
from TradingApp import TradingApp
class SupaBase:
    def __init__(self):
        self.data_to_save = []
        self.data_to_save_currency_status = []
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

    def __debug_this_thread(self):
        pass
        # import pydevd_pycharm
        # pydevd_pycharm.settrace(suspend=True, trace_only_current_thread=True)
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
            ON CONFLICT (date_id, symbol_id)
            DO NOTHING
        """
        try:
            values = []
            for row in self.data_to_save:
                values.append((
                    row["date_id"],
                    row["symbol_id"],
                    datetime.now(),  # updated_at
                    row["sum_ask"],
                    row["sum_bid"],
                    row["difference"],
                    row["count_tick"],
                    row["price_bid"],
                    row["price_ask"],
                    False  # boolean
                ))
            execute_values(self.curr, sql, values)
            self.conn.commit()
        except Exception as e:
            print(f"[ERROR] Falló el insert: {e}")
            self.conn.rollback()

    # def __insert_new_values(self):
    #     insert_sql = """
    #             INSERT INTO "LIQUIDEZTEST" (
    #                 date_id, symbol_id, updated_at, sum_ask, sum_bid,
    #                 difference, count_tick, price_bid, price_ask, boolean
    #             )
    #             VALUES (
    #                 %(date_id)s, %(symbol_id)s, %(updated_at)s, %(sum_ask)s, %(sum_bid)s,
    #                 %(difference)s, %(count_tick)s, %(price_bid)s, %(price_ask)s, %(boolean)s
    #             )
    #             ON CONFLICT (date_id, symbol_id) DO NOTHING
    #         """
    #     try:
    #         for row in self.data_to_save:
    #             self.curr.execute(
    #                 insert_sql,
    #                 {
    #                     "date_id": row["date_id"],
    #                     "symbol_id": row["symbol_id"],
    #                     "updated_at": datetime.now(),
    #                     "sum_ask": row["sum_ask"],
    #                     "sum_bid": row["sum_bid"],
    #                     "difference": row["difference"],
    #                     "count_tick": row["count_tick"],
    #                     "price_bid": row["price_bid"],
    #                     "price_ask": row["price_ask"],
    #                     "boolean": False
    #                 }
    #             )
    #         self.conn.commit()
    #     except Exception as e:
    #         self.conn.rollback()
    #         raise
    def __update_currency_status(self):
        self.__ensure_connection()
        failed_ids = []
        try:
            with self.conn.cursor() as cur:
                for row in self.data_to_save_currency_status: #esto
                    for attempt in range(2):
                        try:
                            cur.execute('''
                                UPDATE "CURRENCYSTATUS"
                                SET "status" = True 
                                WHERE "symbol_id" = %s
                                  AND "date_from" = %s
                                  AND "date_to" = %s;
                            ''', ( int(row['symbol_id']), str(row['date_from']), str(row['date_to'])
                            ))
                            break
                        except Exception as e:
                            print(f"[WARN] Falla actualización ID {str(row['symbol_id'])} {str(row['date_from'])} (intento {attempt+1}): {e}")
                            if attempt != 1:
                                time.sleep(1)

            self.conn.commit()
            print("[INFO] Actualización completada.")
            if failed_ids:
                print(f"[WARN] IDs fallidos luego de 2 intentos: {failed_ids}")
            return 0
        except Exception as e:
            try:
                if self.conn and not self.conn.closed:
                    self.conn.rollback()
            except Exception as rollback_error:
                print(f"[ERROR] Fallo el rollback: {rollback_error}")
            print(f"[ERROR] update_data: {e}")
            return -1
    def receive_and_process_data(self, data : pd.DataFrame, symbol_id : int, date_from, date_to):
        if TradingApp.market_is_closing(date_from, int(symbol_id)) and len(data) == 0: #no me vino nada y el mercado se esta cerrando, esta bien!!
            print("[INFO] SE ESTA GUARDANDO UNA VELA VACIA, NADA EN LIQUIDEZ, SI EN CURRENCY STATUS.")
            new_row_currency_status = {"symbol_id":symbol_id, "date_from":date_from, "date_to":date_to}
            self.data_to_save_currency_status.append(new_row_currency_status)
            return
        chunks_by_second = data.groupby('Time') #error handling
        for second, chunk in chunks_by_second:
            sum_ask = chunk['SizeAsk'].sum()
            sum_bid = chunk['SizeBid'].sum()
            difference = sum_bid - sum_ask
            count_tick = len(chunk)
            last = chunk.loc[chunk.index[-1]]
            price_bid = last['PriceBid'] #Si o si el ultimo precio? un promedio, algo de eso,TODO pensar!!
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
        new_row_currency_status = {"symbol_id":symbol_id, "date_from":date_from, "date_to":date_to}
        self.data_to_save_currency_status.append(new_row_currency_status)

    def fetch_created_data(self, symbol_id, limit=10):
        self.__ensure_connection()
        query = '''
        SELECT DISTINCT ON ("date_from", "date_to", "symbol_id") *
        FROM "CURRENCYSTATUS"
        WHERE "status" = False
          AND "symbol_id" = %s
          AND "date_from" >= TIMESTAMP WITH TIME ZONE '2025-07-07 00:00:00+00:00'
        ORDER BY "date_from" ASC
        LIMIT %s;
        '''
        try:
            with self.conn.cursor() as cur:
                cur.execute(query, (symbol_id, limit))
                rows = cur.fetchall()
                colnames = [desc[0] for desc in cur.description]
                return pd.DataFrame(rows, columns=colnames)
        except Exception as e:
            print(f"[ERROR] fetch_created_data: {e}")
            return pd.DataFrame()
    def fetch_symbol_data(self, symbol_id,):
        self.__ensure_connection()
        query = f'''
        SELECT *
        FROM "SYMBOLS"
        WHERE "symbol_id" = {symbol_id}
        LIMIT 1;
        '''
        try:
            with self.conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                colnames = [desc[0] for desc in cur.description]
                return pd.DataFrame(rows, columns=colnames)
        except Exception as e:
            print(f"[ERROR] fetch_symbol_data: {e}")
            return pd.DataFrame()

    def save_data_to_supabase(self, symbol_id : int):
        print("++++++++++++++++++++++++++++++++++++++++++++")
        print(f"TOTAL AL GUARDAR {len(self.data_to_save)}")
        print("++++++++++++++++++++++++++++++++++++++++++++")
        self.__ensure_connection()
        if len(self.data_to_save) != 0:
            self.__insert_new_values()#CUANDO SE HACE EL SAVE TMB HAY QUE MARCAR CURRENCYSTATUS
        self.__update_currency_status()

        self.data_to_save = []
        self.data_to_save_currency_status = []
    def __del__(self):
        print("MURIO ESTA INSTANCIA")
        try:
            if self.curr:
                self.curr.close()
            if self.conn:
                self.conn.close()
        except Exception as e:
            print(f"[WARN] Error al cerrar recursos: {e}")

