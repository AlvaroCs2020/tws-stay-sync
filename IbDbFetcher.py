import psycopg2
import pandas as pd
import time

class IbDbDataFetcher:
    def __init__(self, db_config, max_retries=3, retry_wait=2):
        self.db_config = db_config
        self.conn = None
        self.max_retries = max_retries
        self.retry_wait = retry_wait
        self.connect()

    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = False
            print("[INFO] Conexión a la base de datos establecida.")
        except psycopg2.OperationalError as e:
            print(f"[ERROR] No se pudo conectar a la base de datos: {e}")
            self.conn = None

    def ensure_connection(self):
        if self.conn is None or self.conn.closed:
            print("[WARN] Conexión cerrada o inválida, reintentando...")
            for attempt in range(self.max_retries):
                try:
                    self.connect()
                    if self.conn and not self.conn.closed:
                        print("[INFO] Reconexión exitosa.")
                        return
                except Exception as e:
                    print(f"[ERROR] Reintento {attempt+1} fallido: {e}")
                    time.sleep(self.retry_wait)
            raise Exception("No se pudo restablecer la conexión a la base de datos.")

    def fetch_created_data(self, symbol_id, limit=10):
        self.ensure_connection()
        query = '''
        SELECT *
        FROM abby."IbIntegration_data"
        WHERE ("STATUS" = 'CREATED')
          AND "SYMBOL_ID" = %s
          AND "DATE_FROM" > TIMESTAMP WITH TIME ZONE '2025-05-01 00:00:00+00:00'
          AND "NW_DAY" = False
        ORDER BY "DATE_FROM" DESC
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

    def fetch_symbol_data(self, symbol_id=1):
        query = f''' 
                SELECT *
                FROM abby."IbIntegration_symbols"
                WHERE "ID" = %s
                LIMIT 1;
                '''
        with self.conn.cursor() as cur:
            cur.execute(query, symbol_id)
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=colnames)

    def update_data(self, df):
        self.ensure_connection()
        failed_ids = []
        try:
            with self.conn.cursor() as cur:
                for _, row in df.iterrows():
                    for attempt in range(2):
                        try:
                            cur.execute('''
                                UPDATE abby."IbIntegration_data"
                                SET "SUM_ASK" = %s,
                                    "SUM_BID" = %s,
                                    "DIFFERENCE" = %s,
                                    "SUM_ASK_STR" = %s,
                                    "SUM_BID_STR" = %s,
                                    "DIFFERENCE_STR" = %s,
                                    "COUNT_TICK" = %s,
                                    "DIFF_LEVEL_ENUM" = %s,
                                    "STATUS" = %s,
                                    "UPDATED_AT" = %s,
                                    "RETRY_COUNT" = %s,
                                    "DOW" = %s,
                                    "NW_DAY" = %s
                                WHERE "ID" = %s;
                            ''', (
                                int(row['SUM_ASK']), int(row['SUM_BID']), int(row['DIFFERENCE']),
                                str(row['SUM_ASK_STR']), str(row['SUM_BID_STR']), str(row['DIFFERENCE_STR']),
                                int(row['COUNT_TICK']), str(row['DIFF_LEVEL_ENUM']), str(row['STATUS']),
                                str(row['UPDATED_AT']), str(row['RETRY_COUNT']), str(row['DOW']), str(row['NW_DAY']),
                                str(row['ID'])
                            ))
                            break
                        except Exception as e:
                            print(f"[WARN] Falla actualización ID {row['ID']} (intento {attempt+1}): {e}")
                            if attempt == 1:
                                failed_ids.append(row['ID'])
                            else:
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

    def close(self):
        if self.conn and not self.conn.closed:
            self.conn.close()
            print("[INFO] Conexión cerrada.")