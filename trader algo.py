from binance.client import Client
from binance.websockets import BinanceSocketManager
import os
from keys_API import key, secret
import pandas as pd
import numpy as np
import talib
from datetime import datetime
import asyncio
import websockets
import datetime
import json
import csv
import multiprocessing
import concurrent.futures


def ema8_ema21():
    eth = pd.read_csv('ETH_file_1h.csv')
    EMA8 = talib.EMA(eth['Close'], timeperiod=8)
    EMA21 = talib.EMA(eth['Close'], timeperiod=21)
    ema = EMA8 - EMA21
    eth["ema8_ema21"] = ema
    df1.to_csv('/Users/shl_alex/Desktop/MTA/untitled/binance-api-trading-algo/ETH_file_1h.csv', index=True)


def sma():                          # ***** 1 *****
    eth = pd.read_csv('ETH_file_2h.csv')
    SMA = talib.SMA(eth['Close'], timeperiod=10)
    eth['sma'] = SMA
    df1.to_csv('/Users/shl_alex/Desktop/MTA/untitled/binance-api-trading-algo/ETH_file_2h.csv', index=True)

    # if eth['Close'].iloc[-1] < eth['Open'].iloc[-1]:
    #     if eth['Close'].iloc[-1] <= SMA.iloc[-1] <= eth['Open'].iloc[-1]:
    #         return 1
    #     else:
    #         return 0
    # else:
    #     return 0

async def ws_stream():
    with open('ETH_file.csv', 'a', newline='') as file:
        uri = "wss://stream.binance.com:9443/ws/ethusdt@kline_3m/ethusdt@kline_1m"
        async with websockets.connect(uri, ping_interval=None) as websocket:
            while True:
                try:
                    msg = await websocket.recv()
                    msg = json.loads(msg)
                    if msg['k']['i'] == '1h' and msg['k']['x'] == True:
                        with open('ETH_file_1h.csv', 'a', newline='') as file:
                            writer = csv.writer(file, delimiter=',')
                            start = datetime.datetime.fromtimestamp(msg['k']['t'] / 1000.0)
                            start = start.strftime("%Y-%m-%d %H:%M:%S")
                            end = datetime.datetime.fromtimestamp(msg['k']['T'] / 1000.0)
                            end = end.strftime("%Y-%m-%d %H:%M:%S")
                            writer.writerow([start, msg['k']['o'], msg['k']['h'], msg['k']['l'], msg['k']['c'], msg['k']['v'], end])
                    if msg['k']['i'] == '3m' and msg['k']['x'] == True:
                        with open('ETH_file_2h.csv', 'a', newline='') as file:
                            writer = csv.writer(file, delimiter=',')
                            start = datetime.datetime.fromtimestamp(msg['k']['t'] / 1000.0)
                            start = start.strftime("%Y-%m-%d %H:%M:%S")
                            end = datetime.datetime.fromtimestamp(msg['k']['T'] / 1000.0)
                            end = end.strftime("%Y-%m-%d %H:%M:%S")
                            writer.writerow([start, msg['k']['o'], msg['k']['h'], msg['k']['l'], msg['k']['c'], msg['k']['v'], end])
                            break
                except Exception as e:
                    print(e)
                    break

def orders():
        # here we are going to set orders for limit buy , stop loss and take profit
    pass

if __name__ == '__main__':
    key = os.environ.get('Demo_API_key')
    secret = os.environ.get('Demo_API_secret_key')
    client = Client(api_key=key, api_secret=secret)
    bm = BinanceSocketManager(client)
    client.API_URL = 'https://testnet.binance.vision/api'

    # get historical info about the asset
    kline_1h = client.get_historical_klines(symbol='ETHUSDT', interval='1h', start_str='3 day ago UTC', end_str='now UTC')
    kline_2h = client.get_historical_klines(symbol='ETHUSDT', interval='2h', start_str='3 day ago UTC', end_str='now UTC')


    # write the data on CSV file
    df1 = pd.DataFrame(kline_1h, columns=['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time',
                                      'Quote asset volume', 'Number of trades', 'Taker buy base asset volume',
                                      'Taker buy quote asset volume', 'Can be ignored'])

    df2 = pd.DataFrame(kline_2h, columns=['Open time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close time',
                                      'Quote asset volume', 'Number of trades', 'Taker buy base asset volume',
                                      'Taker buy quote asset volume', 'Can be ignored'])

    # change the time from MS to UTC  && convert to local time zone
    df1['Open time'] = pd.to_datetime(df1['Open time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Asia/Tel_Aviv')
    df2['Open time'] = pd.to_datetime(df2['Open time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Asia/Tel_Aviv')
    df1['Open time'] = df1['Open time'].astype(str).str[:-6]
    df2['Open time'] = df2['Open time'].astype(str).str[:-6]
    df1['Close time'] = pd.to_datetime(df1['Close time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Asia/Tel_Aviv')
    df2['Close time'] = pd.to_datetime(df2['Close time'], unit='ms').dt.tz_localize('UTC').dt.tz_convert('Asia/Tel_Aviv')
    df1['Close time'] = df1['Close time'].astype(str).str[:-6]
    df2['Close time'] = df2['Close time'].astype(str).str[:-6]

    # Delete some unuseful columns from the df
    df1.drop(['Taker buy base asset volume', 'Taker buy quote asset volume', 'Can be ignored', 'Number of trades',
             'Quote asset volume'], axis=1, inplace=True)
    df2.drop(['Taker buy base asset volume', 'Taker buy quote asset volume', 'Can be ignored', 'Number of trades',
              'Quote asset volume'], axis=1, inplace=True)

    df1.drop(df1.tail(1).index, inplace=True)
    df2.drop(df2.tail(1).index, inplace=True)

    # Creating a csv file
    df1.set_index('Open time', inplace=True)
    df1['Open'] = df1['Open'].astype(float)
    df1['Close'] = df1['Close'].astype(float)
    df1.to_csv('/Users/shl_alex/Desktop/MTA/untitled/binance-api-trading-algo/ETH_file_1h.csv', index=True)

    df2.set_index('Open time', inplace=True)
    df2['Open'] = df2['Open'].astype(float)
    df2['Close'] = df2['Close'].astype(float)
    df2.to_csv('/Users/shl_alex/Desktop/MTA/untitled/binance-api-trading-algo/ETH_file_2h.csv', index=True)


    while True:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(ws_stream())
        with concurrent.futures.ProcessPoolExecutor() as executor:
            p1 = executor.submit(sma)
            p2 = executor.submit(ema8_ema21)
            # p2 = executor.submit(macd)
            # p3 = executor.submit(MFI)
        #     print(p1.result(), p2.result(), p3.result())
        # eth = pd.read_csv('ETH_file.csv')
        # print(eth['Open time'].iloc[-1])



