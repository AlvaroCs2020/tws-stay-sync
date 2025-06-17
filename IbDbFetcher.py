from pickletools import long1

import psycopg2
import pandas as pd
import time
class IbDbDataFetcher:
    def __init__(self, db_config):
        """
        db_config debe ser un diccionario con las claves:
        dbname, user, password, host, port
        """
        self.conn = psycopg2.connect(**db_config)
    def reconnect(self, db_config):
        self.conn = psycopg2.connect(**db_config)
    def fetch_created_data(self, symbol_id=1, limit=10):
        query = f'''
        SELECT *
        FROM abby."IbIntegration_data"
        WHERE "STATUS" = 'CREATED'
          AND "SYMBOL_ID" = %s
          AND "DATE_FROM" < TIMESTAMP WITH TIME ZONE '2025-06-01 00:00:00+00:00'
          AND "DATE_FROM" > TIMESTAMP WITH TIME ZONE '2025-05-01 00:00:00+00:00'
          AND "NW_DAY" = False
        ORDER BY "DATE_FROM" DESC
        LIMIT %s;
        '''
        ##Test query
        # query = f'''
        #         SELECT *
        #         FROM abby."IbIntegration_data"
        #         WHERE "STATUS" = 'SUCCESS'
        #           AND "SYMBOL_ID" = %s
        #           AND "DATE_FROM" < TIMESTAMP WITH TIME ZONE '2025-05-28 2:40:00+00:00'
        #           AND "NW_DAY" = False
        #         ORDER BY "DATE_FROM" DESC
        #         LIMIT %s;
        #         '''
        with self.conn.cursor() as cur:
            cur.execute(query, (symbol_id, limit))
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
        return pd.DataFrame(rows, columns=colnames)

    def close(self):
        self.conn.close()

    def update_data(self, df):

        failed_ids = []
        try:
            with self.conn.cursor() as cur:
                for _, row in df.iterrows():
                    for attempt in range(2):
                        try:
                            cur.execute('''
                                        UPDATE abby."IbIntegration_data"
                                        SET "SUM_ASK"         = %s,
                                            "SUM_BID"         = %s,
                                            "DIFFERENCE"      = %s,
                                            "SUM_ASK_STR"     = %s,
                                            "SUM_BID_STR"     = %s,
                                            "DIFFERENCE_STR"  = %s,
                                            "COUNT_TICK"      = %s,
                                            "DIFF_LEVEL_ENUM" = %s,
                                            "STATUS"          = %s,
                                            "UPDATED_AT"      = %s,
                                            "RETRY_COUNT"     = %s,
                                            "DOW"             = %s,
                                            "NW_DAY"          = %s
                                        WHERE "ID" = %s;
                                        ''', (
                                                int(row['SUM_ASK']), int(row['SUM_BID']), int(row['DIFFERENCE']),
                                                str(row['SUM_ASK_STR']), str(row['SUM_BID_STR']), str(row['DIFFERENCE_STR']),
                                                int(row['COUNT_TICK']), str(row['DIFF_LEVEL_ENUM']), str(row['STATUS']),
                                                str(row['UPDATED_AT']), str(row['RETRY_COUNT']), str(row['DOW']), str(row['NW_DAY']),
                                                str(row['ID'])
                                            ))
                            break  # éxito
                        except Exception as e:
                            print(f"[Intento {attempt + 1}] Error al actualizar ID {row['ID']}: {e}")
                            if attempt == 1:
                                failed_ids.append(row['ID'])
                            else:
                                time.sleep(1)
                self.conn.commit()
                print(f"Actualización completada con éxito.")
        except psycopg2.InterfaceError as e:
            print(f"Error de conexión: {e}")
            return -1
        except Exception as e:
            self.conn.rollback()
            print(f"Error general al actualizar el DataFrame: {e}")
            return -1

        if failed_ids:
            print("IDs fallidos luego de 2 intentos:", failed_ids)
        return 0
# fetcher = IbIntegrationDataFetcher(db_config)
# df = fetcher.fetch_created_data(limit=5)
# print(df)
# fetcher.close()