import time
import threading
from datetime import datetime
from typing import Dict, Optional
import pandas as pd
import warnings
import random
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.wrapper import *
from ibapi.contract import Contract
from ibapi.order import Order
from ibapi.common import *
from enum import Enum
class TradingApp(EClient, EWrapper):

    def __init__(self, symbol, sec_type, currency, exchange) -> None:
        self.symbol   = symbol  # 'EUR'
        self.sec_type = sec_type
        self.exchange = exchange
        self.currency = currency
        EClient.__init__(self, self)
        self.last_tick_count = 0
        self.req_made = False
        self.req_id = random.randint(1, 10000)
        self.df_saved_ticks  = pd.DataFrame(columns=[
            'Time', 'TickAttriBidAsk', 'AskPastHigh', 'PriceBid',
            'PriceAsk', 'SizeBid', 'SizeAsk', 'TimeFormatted'
        ])
        self.data = self.df_saved_ticks
        self.nextOrderId: Optional[int] = None

    def error(self, reqId: int, errorCode: int, errorString: str) -> None:

        print(f"Error: {reqId}, {errorCode}, {errorString}")
        if errorCode == 102: #cambiamos el req id
            self.req_id = random.randint(1, 10000)

    def nextValidId(self, orderId: int) -> None:

        super().nextValidId(orderId)
        self.nextOrderId = orderId

    def get_historical_data(self, reqId: int, contract: Contract) -> pd.DataFrame:

        self.data = pd.DataFrame(columns=["time", "high", "low", "close"])
        self.data.set_index("time", inplace=True)
        self.reqHistoricalData(
            reqId=reqId,
            contract=contract,
            endDateTime="",
            durationStr="1 D",
            barSizeSetting="1 min",
            whatToShow="MIDPOINT",
            useRTH=0,
            formatDate=2,
            keepUpToDate=False,
            chartOptions=[],
        )
        time.sleep(5)
        return self.data

    def historicalTicks(self, reqId: int, ticks: ListOfHistoricalTickLast, done: bool):
        print("no esta viniendo nada para mi ticks: " + str(len(ticks)))
    def historicalTicks(self, reqId: int, ticks: ListOfHistoricalTickBidAsk, done: bool):
        print("no esta viniendo nada para mi tick last : " + str(len(ticks)))

        # for tick in ticks:
        #     print("HistoricalTickLast. ReqId:", reqId, tick)

    def historicalData(self, reqId: int, bar: BarData) -> None:

        df = self.data

        df.loc[
            pd.to_datetime(bar.date, unit="s"),
            ["high", "low", "close"]
        ] = [bar.high, bar.low, bar.close]

        df = df.astype(float)

        self.data = df

    @staticmethod
    def get_contract(symbol: str) -> Contract:

        contract = Contract()
        contract.symbol = symbol
        contract.secType = "STK"
        contract.exchange = "SMART"
        contract.currency = "USD"
        return contract
    def get_forex_contract(self) -> Contract:
        """
        Devuelve un contrato de tipo CASH para operar en Forex.

        Ejemplo: pair='EURUSD'
        """
        contract = Contract()
        contract.symbol = self.symbol  # 'EUR'
        contract.secType = self.sec_type
        contract.exchange = self.exchange
        contract.currency = self.currency
        return contract

    def place_order(self, contract: Contract, action: str, order_type: str, quantity: int) -> None:

        order = Order()
        order.action = action
        order.orderType = order_type
        order.totalQuantity = quantity

        self.placeOrder(self.nextOrderId, contract, order)
        self.nextOrderId += 1
        print("Order placed")
    def historicalTicksBidAsk(self, reqId: int, ticks: ListOfHistoricalTickBidAsk, done: bool):

        parsed_list = []
        for tick in ticks:
            raw = str(tick)

            # Separar y convertir en dict
            parsed = {}
            for pair in raw.split(", "):
                if ": " in pair:
                    key, value = pair.split(": ", 1)
                    try:
                        parsed[key] = float(value) if '.' in value else int(value)
                    except ValueError:
                        parsed[key] = value  # por si hay strings
            parsed_list.append(parsed)
        df = pd.DataFrame(parsed_list)
        max_time = str( pd.to_datetime(df['Time'].max(), unit='s', utc=True))
        min_time = str( pd.to_datetime(df['Time'].min(), unit='s', utc=True))
        print("ticks bid ask: " + str(len(ticks)) +" : "+ str(done) + " : " +str(reqId) + f" from {min_time} to {max_time}")
        self.req_made = True
        self.data = df
# 20250528-04:15:00
    def get_historical_data_by_tick(self, contract: Contract, start_time : str, end_time : str) -> pd.DataFrame:#"20250528-04:15:00"
        self.reqHistoricalTicks(self.req_id, contract, start_time, end_time, 1000, "BID_ASK", 1, False, [])
        self.data = pd.DataFrame(columns=['Time', 'TickAttriBidAsk', 'AskPastHigh', 'PriceBid', 'PriceAsk', 'SizeBid', 'SizeAsk'])
        self.data.set_index("Time", inplace=True)
        time.sleep(2.5)

        return self.data

    def get_ticks_per_bar(self, start_time : str, end_time : str):
        contract_eurusd = self.get_forex_contract()
        stop_time = end_time
        stop_time_dt = pd.to_datetime(stop_time, utc=True)
        start_time_dt = pd.to_datetime(start_time, utc=True)
        boolean = True
        count = 0
        self.req_made = False
        df = self.get_historical_data_by_tick(contract_eurusd, start_time, end_time)

        while True:  # Le pegamos a TWS hasta que devuelva algo
            count+=1
            if len(df) > 0 or count > 4:
                break
            df = self.get_historical_data_by_tick(contract_eurusd, start_time, end_time)
        self.last_tick_count = len(df)
        if len(df) == 0: #Si no anduvo, lo marcamos
            return self.df_saved_ticks
        try:
            df['TimeFormatted'] = pd.to_datetime(df['Time'], unit='s', utc=True)
            max_time = df['TimeFormatted'].max()
        except KeyError:
            print("Volvio a pasar el error de verga este")
            return self.df_saved_ticks
        #te clavas 2 segundos
        pd_end_time=pd.to_datetime(end_time, utc=True)

        not_done = max_time < pd_end_time

        list_of_chunks = []
        list_of_chunks.append(df)

        if not_done: #si no entro la barra entera en 1000 ticks, entonces hay que qeudarnos con el ultimo segundo completo y pedir a tws desde el proximo
            df = df[df['TimeFormatted'] < max_time]

        count = 0
        while max_time < pd_end_time and count < 7:
            self.req_made = False
            begin_of_chunk = max_time
            new_start_time = max_time.strftime('%Y%m%d-%H:%M:%S')
            seconds = max_time.second
            rounded_seconds = seconds -1
            new_start_time_rounded = max_time.replace(second=rounded_seconds, microsecond=0).strftime('%Y%m%d-%H:%M:%S')
            df_temp = self.df_saved_ticks
            df_temp = self.get_historical_data_by_tick(contract_eurusd, new_start_time_rounded, end_time)

            while True:  # Le pegamos a TWS hasta que devuelva algo
                count += 1
                if len(df_temp) > 0 or count > 4:
                    count = 0
                    break
                df_temp = self.get_historical_data_by_tick(contract_eurusd, new_start_time_rounded, end_time)

            try:
                df_temp['TimeFormatted'] = pd.to_datetime(df_temp['Time'], unit='s', utc=True)
                new_max = df_temp['TimeFormatted'].max()
                if new_max < pd_end_time: #Este no va a ser el ultimo tramo de ticks que conformen la vela, lo recortamos y actualizamos el max
                    df_temp = df_temp[df_temp['TimeFormatted'] < new_max]
                else:
                    not_done = False
                max_time = new_max
                df_temp = df_temp[df_temp['TimeFormatted'] >= begin_of_chunk]  # le recortamos los que este antes del inicio y guardamos
                list_of_chunks.append(df_temp)
            except KeyError:
                print("Volvia a pasar el error de verga este " + str(count))
                return self.df_saved_ticks
        if not_done:
            return self.df_saved_ticks
        df_filtered_1min = pd.concat(list_of_chunks, ignore_index=True)
        df_filtered_1min = df_filtered_1min[df_filtered_1min['TimeFormatted'] < stop_time_dt]
        df_filtered_1min = df_filtered_1min[df_filtered_1min['TimeFormatted'] >= start_time_dt]

        return df_filtered_1min
    def convert_values_to_str(self, sum_ask,sum_bid):
        difference = sum_bid - sum_ask

        if(difference == 0):
            diff_level = "NONE"
        elif (abs(difference) < 30 * 1000000):
            diff_level = "LOW"
        elif (abs(difference) < 60 * 1000000):
            diff_level = "MEDIUM"
        elif (abs(difference) < 1500 * 1000000):
            diff_level = "HIGH"
        else:
            diff_level = "SUPER_HIGH"
        return diff_level
    @staticmethod
    def format_int_to_string(num):
        if num >= 1_000_000_000:
            return f"{num / 1_000_000_000:.3f} B"
        elif num >= 1_000_000:
            return f"{num / 1_000_000:.3f} M"
        elif num >= 1_000:
            return f"{num / 1_000:.3f} K"
        else:
            return str(num)