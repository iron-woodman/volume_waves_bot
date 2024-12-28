import concurrent.futures
from binance.client import Client
from datetime import datetime
import threading
from src.telegram_api import send_signal
from src.config_handler import TLG_TOKEN, TLG_CHANNEL_ID
from src.config_handler import BINANCE_API_KEY, BINANCE_Secret_KEY
import src.logger as custom_logging


client = Client(BINANCE_API_KEY, BINANCE_Secret_KEY)

def process_symbol(symbol):
    """Обработка одного символа в отдельном потоке."""
    try:
        interval = "1h"
        limit = 27
        klines = client.futures_klines(symbol=symbol, interval=interval, limit=limit)


        if len(klines) < 27:
            print(f"  [{threading.current_thread().name}] Получено меньше 27 баров для {symbol}: {len(klines)}. Проверьте наличие данных.")
            return None # Возвращаем None если баров недостаточно

        three_hour_bars = []
        for i in range(0, 27, 3):
            open_time_ms = klines[i][0]
            open_time = datetime.fromtimestamp(open_time_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')
            open_price = float(klines[i][1])
            high_price = max(float(klines[j][2]) for j in range(i, i + 3))
            low_price = min(float(klines[j][3]) for j in range(i, i + 3))
            close_price = float(klines[i + 2][4])
            volume = round(sum(float(klines[j][5]) for j in range(i, i + 3)))
            three_hour_bars.append([open_time, open_price, high_price, low_price, close_price, volume])

        print(f"  [{threading.current_thread().name}] Трехчасовые бары для {symbol}:")
        for bar in three_hour_bars:
            print(f"    {bar}")
        return three_hour_bars # Возвращаем результат

    except Exception as e:
        print(f"  [{threading.current_thread().name}] Произошла ошибка при обработке {symbol}: {e}")
        return None # Возвращаем None при ошибке


def calculate_buying_volume(open, high, low, close, volume):
    """
    объем покупок
    """
    if high == low:
        return 0
    buying_volume = volume * (close - low) / (high- low)
    return buying_volume


def calculate_selling_volume(open, high, low, close, volume):
    """
    объем продаж
    """
    if high == low:
        return 0
    selling_volume = volume * (high-close) / (high- low)
    return selling_volume


def check_for_buy_pattern(bars, symbol):
    """
    Функция-фильтр для проверки комбинации баров.

    Args:
        bars: Список трехчасовых баров для одного символа.

    Returns:
        True, если бары прошли проверку, False в противном случае.
    """
    start_index = 0
    num_candles = 3
    cumulative_positive_array = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    cumulative_negative_array = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

    for i in range(len(bars)):
        if i == start_index or i - start_index >= num_candles:
            cumulative_positive = 0.0
            cumulative_negative = 0.0
            start_index = i
        buying_volume = calculate_buying_volume(bars[i][1], bars[i][2], bars[i][3], bars[i][4], bars[i][5])
        selling_volume = calculate_selling_volume(bars[i][1], bars[i][2], bars[i][3], bars[i][4], bars[i][5])

        if buying_volume > selling_volume:
            cumulative_positive += buying_volume - selling_volume
        else:
            cumulative_negative += selling_volume - buying_volume

        cumulative_positive_array[i] = cumulative_positive
        cumulative_negative_array[i] = cumulative_negative

    avg_cumulative_positive = sum(cumulative_positive_array) / len(cumulative_positive_array)
    avg_cumulative_negative = sum(cumulative_negative_array) / len(cumulative_negative_array)

    long = False
    short = False

    for i in range(0, 9 - num_candles * 2):
        if ((cumulative_positive_array[i] > cumulative_positive_array[i + num_candles] >
             cumulative_positive_array[i + num_candles * 2]) and cumulative_positive_array[
            i] > avg_cumulative_positive and
                cumulative_positive_array[i + num_candles * 2] > 0.1 * cumulative_positive_array[i + num_candles]):
            # print(cumulative_positive_array[i], cumulative_positive_array[i + 3], cumulative_positive_array[i + 6])
            custom_logging.info(f"{symbol}:SHORT:AVG={avg_cumulative_positive}\t({i};{cumulative_positive_array[i]}), "
                         f"({[i + 3]};{cumulative_positive_array[i + 3]}),"
                         f"({i + 6};{cumulative_positive_array[i + 6]})")
            short = True
        elif ((cumulative_negative_array[i] > cumulative_negative_array[i + num_candles] >
               cumulative_negative_array[i + num_candles * 2]) and cumulative_negative_array[
                  i] > avg_cumulative_negative and
              cumulative_negative_array[i + num_candles * 2] > 0.1 * cumulative_negative_array[i + num_candles]):
            # print(cumulative_negative_array[i], cumulative_negative_array[i + 3], cumulative_negative_array[i + 6])
            custom_logging.info(f"{symbol}:LONG:AVG={avg_cumulative_negative}\t({i};{cumulative_negative_array[i]}), "
                         f"({[i + 3]};{cumulative_negative_array[i + 3]}),"
                         f"({i + 6};{cumulative_negative_array[i + 6]})")
            long = True

    if long==True and short==False:
        return True
    else:
        return False




def check_for_sell_pattern(bars, symbol):
    """
    Функция-фильтр для проверки комбинации баров.

    Args:
        bars: Список трехчасовых баров для одного символа.

    Returns:
        True, если бары прошли проверку, False в противном случае.
    """
    # Здесь будет реализована ваша логика проверки комбинации баров
    start_index = 0
    num_candles = 3
    cumulative_positive_array = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]
    cumulative_negative_array = [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]

    for i in range(len(bars)):
        if i == start_index or i - start_index >= num_candles:
            cumulative_positive = 0.0
            cumulative_negative = 0.0
            start_index = i
        buying_volume = calculate_buying_volume(bars[i][1], bars[i][2], bars[i][3], bars[i][4], bars[i][5])
        selling_volume = calculate_selling_volume(bars[i][1], bars[i][2], bars[i][3], bars[i][4], bars[i][5])

        if buying_volume > selling_volume:
            cumulative_positive += buying_volume - selling_volume
        else:
            cumulative_negative += selling_volume - buying_volume

        cumulative_positive_array[i] = cumulative_positive
        cumulative_negative_array[i] = cumulative_negative

    avg_cumulative_positive = sum(cumulative_positive_array) / len(cumulative_positive_array)
    avg_cumulative_negative = sum(cumulative_negative_array) / len(cumulative_negative_array)

    long = False
    short = False

    for i in range(0, 9 - num_candles * 2):
        if ((cumulative_positive_array[i] > cumulative_positive_array[i + num_candles] >
             cumulative_positive_array[i + num_candles * 2]) and cumulative_positive_array[
            i] > avg_cumulative_positive and
                cumulative_positive_array[i + num_candles * 2] > 0.1 * cumulative_positive_array[i + num_candles]):
            # print(cumulative_positive_array[i], cumulative_positive_array[i + 3], cumulative_positive_array[i + 6])
            custom_logging.info(f"{symbol}:SHORT:AVG={avg_cumulative_positive}\t({i};{cumulative_positive_array[i]}), "
                         f"({[i + 3]};{cumulative_positive_array[i + 3]}),"
                         f"({i + 6};{cumulative_positive_array[i + 6]})")
            short = True
        elif ((cumulative_negative_array[i] > cumulative_negative_array[i + num_candles] >
               cumulative_negative_array[i + num_candles * 2]) and cumulative_negative_array[
                  i] > avg_cumulative_negative and
              cumulative_negative_array[i + num_candles * 2] > 0.1 * cumulative_negative_array[i + num_candles]):
            # print(cumulative_negative_array[i], cumulative_negative_array[i + 3], cumulative_negative_array[i + 6])
            custom_logging.info(f"{symbol}:LONG:AVG={avg_cumulative_negative}\t({i};{cumulative_negative_array[i]}), "
                         f"({[i + 3]};{cumulative_negative_array[i + 3]}),"
                         f"({i + 6};{cumulative_negative_array[i + 6]})")
            long = True

    if short == True and long == False:
        return True
    else:
        return False


if __name__ == "__main__":

    spot = False
    custom_logging.info("**Бот запущен**")
    # send_signal("**Бот запущен**", TLG_TOKEN, TLG_CHANNEL_ID)
    try:
        if spot:
            exchange_info = client.get_exchange_info()
        else:
            exchange_info = client.futures_exchange_info()

        if spot:
            symbols = [symbol['symbol'] for symbol in exchange_info['symbols'] if "USDT" in symbol['symbol']]
        else:
            symbols = [symbol['symbol'] for symbol in exchange_info['symbols'] if symbol['contractType'] == 'PERPETUAL'
                       and "USDT" in symbol['symbol']]

        max_workers = 4 # Максимальное количество одновременно работающих потоков (настройте по необходимости)
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = executor.map(process_symbol, symbols)

            buy_signal_symbols = []
            sell_signal_symbols = []

            for symbol, bars in zip(symbols, results):  # распаковываем результаты
                if bars is not None and check_for_buy_pattern(bars, symbol):
                    buy_signal_symbols.append(symbol)
                if bars is not None and check_for_sell_pattern(bars, symbol):
                    sell_signal_symbols.append(symbol)

            if len(buy_signal_symbols) > 0:
                buy_signal_symbols.sort()
                signal_str = '**Монеты на продажу:**\n\n'
                for coin in buy_signal_symbols:
                    signal_str += f'{coin}\n'
                send_signal(signal_str, TLG_TOKEN, TLG_CHANNEL_ID)

            if len(sell_signal_symbols) > 0:
                sell_signal_symbols.sort()
                signal_str = '**Монеты на покупку:**\n\n'
                for coin in sell_signal_symbols:
                    signal_str += f'{coin}\n'
                send_signal(signal_str, TLG_TOKEN, TLG_CHANNEL_ID)

    except Exception as e:
        print(f"Произошла глобальная ошибка: {e}")
