## -*- coding: utf-8 -*-
from binance import Client
import src.logger as custom_logging
from src.config_handler import BINANCE_API_KEY, BINANCE_Secret_KEY


def load_futures_list():
    futures = []

    try:
        client = Client(BINANCE_API_KEY, BINANCE_Secret_KEY)
        futures_info_list = client.futures_exchange_info()
        for item in futures_info_list['symbols']:
            if item['status'] != 'TRADING' or item['contractType'] != 'PERPETUAL': continue
            if item['pair'].endswith('USDT'):
                futures.append(item['pair'])
        print('FUTURES:', futures)
    except Exception as e:
        custom_logging.error(f"load_futures_list exception: {e}")
    return futures


def load_spot_list():
    coins = []
    try:
        client = Client(BINANCE_API_KEY, BINANCE_Secret_KEY)
        coins_info_list = client.get_exchange_info()
        for item in coins_info_list['symbols']:
            if item['status'] != 'TRADING' or item['quoteAsset'] != 'USDT': continue
            coins.append(item['symbol'])
        print('SPOT:', coins)
    except Exception as e:
        custom_logging.error(f"load_spot_list exception: {e}")
    return coins


# futures_list = load_futures_list()
# spot_list = load_spot_list()
