import time
import threading
from time import sleep
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
from datetime import time


class TradingApp(EClient, EWrapper):
    _instance = None
    _initialized = False
    _contract_info_by_id: Dict[int, Dict] = {}

    def __new__(cls, contract_info_by_id=None):
        if cls._instance is None:
            cls._instance = super(TradingApp, cls).__new__(cls)
            if contract_info_by_id:
                cls._contract_info_by_id = contract_info_by_id
        return cls._instance

    def __init__(self, contract_info_by_id: Dict[int, Dict] = None) -> None:
        if self._initialized:
            return
        self._initialized = True
        self.contract_info_by_id = self._contract_info_by_id
        EClient.__init__(self, self)
        self.last_tick_count = 0
        self.req_made = False
        self.req_id = random.randint(1, 10000)
        self.df_empty = pd.DataFrame(columns=[
            'Time', 'TickAttriBidAsk', 'AskPastHigh', 'PriceBid',
            'PriceAsk', 'SizeBid', 'SizeAsk', 'TimeFormatted'
        ])
        self.data = self.df_empty
        self.nextOrderId: Optional[int] = None

    def error(self, reqId: int, errorCode: int, errorString: str) -> None:
        print(f"Error: {reqId}, {errorCode}, {errorString}")
        if errorCode == 102:
            self.req_id = random.randint(1, 10000)

    def change_id(self):
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
        sleep(5)
        return self.data

    def historicalTicks(self, reqId: int, ticks: ListOfHistoricalTickLast, done: bool):
        print("no esta viniendo nada para mi ticks: " + str(len(ticks)))

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

    @classmethod
    def get_forex_contract(cls, symbol_id: int) -> Contract:
        info = cls._contract_info_by_id[symbol_id]
        contract = Contract()
        contract.symbol = info['symbol']
        contract.secType = info['sec_type']
        contract.exchange = info['exchange']
        contract.currency = info['currency']
        return contract

    @classmethod
    def market_is_closing(cls,ts, symbol_id):
        if isinstance(ts, pd.Timestamp):
            ts = ts.to_pydatetime()
        t = ts.time()
        info = cls._contract_info_by_id[symbol_id]
        close_hour = int(info['close_hour'])
        return time((close_hour-1), 59) <= t < time((close_hour+1), 00) or (ts.isoweekday() == 5 and time((close_hour-1), 59) <= t)

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
            parsed = {}
            for pair in raw.split(", "):
                if ": " in pair:
                    key, value = pair.split(": ", 1)
                    try:
                        parsed[key] = float(value) if '.' in value else int(value)
                    except ValueError:
                        parsed[key] = value
            parsed_list.append(parsed)
        df = pd.DataFrame(parsed_list)
        try:
            max_time = str(pd.to_datetime(df['Time'].max(), unit='s', utc=True))
            min_time = str(pd.to_datetime(df['Time'].min(), unit='s', utc=True))
        except KeyError:
            self.data = pd.DataFrame()
            return
        print("ticks bid ask: " + str(len(ticks)) + " : " + str(done) + " : " + str(reqId) + f" from {min_time} to {max_time}")
        
        self.req_made = True
        self.data = df

    def get_historical_data_by_tick(self, contract: Contract, start_time: str, end_time: str) -> pd.DataFrame:
        self.reqHistoricalTicks(self.req_id, contract, start_time, end_time, 1000, "BID_ASK", 1, False, [])
        self.data = pd.DataFrame(columns=['Time', 'TickAttriBidAsk', 'AskPastHigh', 'PriceBid', 'PriceAsk', 'SizeBid', 'SizeAsk'])
        self.data.set_index("Time", inplace=True)
        sleep(3.5)
        print(f"estamos pasando por aca {self.data['Time'].min()}{self.data['Time'].max()}")
        return self.data

    def get_ticks_per_bar(self, start_time: str, end_time: str, symbol_id: int):
        contract_by_symbol = self.get_forex_contract(symbol_id)
        stop_time_dt = pd.to_datetime(end_time, utc=True)
        start_time_dt = pd.to_datetime(start_time, utc=True)
        self.req_made = False
        print("Se hace consulta principal")
        df = self.get_historical_data_by_tick(contract_by_symbol, start_time, end_time)
        self.last_tick_count = len(df)
        if self.last_tick_count == 0:
            return self.df_empty
        try:
            df['TimeFormatted'] = pd.to_datetime(df['Time'], unit='s', utc=True)
            max_time = df['TimeFormatted'].max()
        except KeyError:
            print("Volvio a pasar el error de verga este, se murio en la primer consulta")
            return self.df_empty

        not_done = not (max_time >= (stop_time_dt - pd.Timedelta(seconds=1)))
        list_of_chunks = [df]
        if not_done:
            df = df[df['TimeFormatted'] < max_time]

        last_new_start_time_rounded = ''
        while not_done:
            print("Entro al ciclo de min not note")
            begin_of_chunk = max_time
            new_start_time = max_time.strftime('%Y%m%d-%H:%M:%S')
            rounded_seconds = max_time.second
            new_start_time_rounded = max_time.replace(second=rounded_seconds, microsecond=0).strftime('%Y%m%d-%H:%M:%S')
            if new_start_time_rounded == last_new_start_time_rounded or pd.to_datetime(new_start_time, utc=True) < start_time_dt:
                print(f"[WARN] Muy pocos ticks no se esta pudiendo completar 1 seg o se esta yendo el inicio {pd.to_datetime(new_start_time, utc=True) < start_time_dt}]")
                self.req_made = False
                return self.df_empty
            if pd.to_datetime(new_start_time, utc=True) > stop_time_dt :
                sleep(10)
                break
            self.req_made = False
            print(f"Se hace consulta desde el bucle de seg no completo {pd.to_datetime(new_start_time, utc=True)} {stop_time_dt}")
            df_temp = self.get_historical_data_by_tick(contract_by_symbol, new_start_time_rounded, end_time)
            last_new_start_time_rounded = new_start_time_rounded
            try:
                print("Esto me sirve como punto de control")
                df_temp['TimeFormatted'] = pd.to_datetime(df_temp['Time'], unit='s', utc=True)
                new_max = df_temp['TimeFormatted'].max()
                df_temp = df_temp[df_temp['TimeFormatted'] < new_max]
                max_time = new_max
                df_temp = df_temp[df_temp['TimeFormatted'] >= begin_of_chunk]
                list_of_chunks.append(df_temp)
                if new_max > stop_time_dt:
                    break
            except KeyError:
                print("Volvia a pasar el error de verga este ")
                return self.df_empty

        df_filtered_1min = pd.concat(list_of_chunks, ignore_index=True)
        df_filtered_1min = df_filtered_1min[df_filtered_1min['TimeFormatted'] < stop_time_dt]
        df_filtered_1min = df_filtered_1min[df_filtered_1min['TimeFormatted'] >= start_time_dt]
        return df_filtered_1min

    @staticmethod
    def convert_values_to_str(sum_ask, sum_bid):
        difference = sum_bid - sum_ask
        if difference == 0:
            diff_level = "NONE"
        elif abs(difference) < 30 * 1000000:
            diff_level = "LOW"
        elif abs(difference) < 60 * 1000000:
            diff_level = "MEDIUM"
        elif abs(difference) < 1500 * 1000000:
            diff_level = "HIGH"
        else:
            diff_level = "SUPER_HIGH"
        return diff_level

    @staticmethod
    def format_int_to_string(num):
        def fmt(n):
            return f"{n:.1f}".rstrip("0").rstrip(".")
        abs_num = abs(num)
        sign = "-" if num < 0 else ""
        if abs_num >= 1_000_000_000:
            return f"{sign}{fmt(abs_num / 1_000_000_000)} B"
        elif abs_num >= 1_000_000:
            return f"{sign}{fmt(abs_num / 1_000_000)} M"
        elif abs_num >= 1_000:
            return f"{sign}{fmt(abs_num / 1_000)} K"
        else:
            return f"{num}"
