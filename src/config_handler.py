## -*- coding: utf-8 -*-
import os
from dotenv import load_dotenv

load_dotenv()

BINANCE_API_KEY = os.getenv('BINANCE_API_KEY')
BINANCE_Secret_KEY = os.getenv('BINANCE_SECRET_KEY')

TLG_TOKEN = os.getenv('TLG_TOKEN')
TLG_CHANNEL_ID = os.getenv('TLG_CHANNEL_ID')
# TLG_CHANNEL_ID = -1001782999630

TIMEFRAMES = ['3h']
