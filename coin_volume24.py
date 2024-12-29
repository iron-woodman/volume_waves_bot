import json
import logging
import threading
from binance import Client
from binance.exceptions import BinanceAPIException
from src.config_handler import BINANCE_API_KEY, BINANCE_Secret_KEY
from queue import Queue
from typing import Tuple, Optional, Dict

# Настройка логирования
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def get_volume(client: Client, symbol: str) -> Tuple[str, Optional[float]]:
    """
    Получает объем торгов для конкретной пары.

    Args:
        client: Синхронный клиент Binance.
        symbol: Торговая пара.

    Returns:
       Кортеж содержащий название символа и объем или None если произошла ошибка.
    """
    try:
        ticker = client.futures_ticker(symbol=symbol)
        if ticker:
            volume = float(ticker["volume"])
            logging.debug(f"Получен объем для {symbol}: {volume}")
            return symbol, volume
        else:
            logging.warning(f"Не удалось получить данные для {symbol}")
            return symbol, None
    except BinanceAPIException as e:
        logging.error(f"Ошибка при получении данных для {symbol}: {e}")
        return symbol, None
    except Exception as e:
        logging.error(f"Непредвиденная ошибка при получении данных для {symbol}: {e}")
        return symbol, None


def worker(client: Client, symbol_queue: Queue, all_data: Dict[str, float]):
    """
      Поток обработчик для получения объема символа.
    """
    while True:
        symbol = symbol_queue.get()
        if symbol is None:
            break

        symbol, volume = get_volume(client, symbol)
        if volume is not None:
            all_data[symbol] = volume
        symbol_queue.task_done()


def get_usdt_futures_volume_json():
    """
    Получает суточный объем торгуемых USDT фьючерсов на Binance
    и сохраняет их в файл в формате JSON (словарь coin:volume).
    """
    all_data: Dict[str, float] = {}
    symbol_queue = Queue()
    try:
        client = Client(BINANCE_API_KEY, BINANCE_Secret_KEY)
        exchange_info = client.futures_exchange_info()
        if exchange_info:
            all_symbols = [symbol['symbol'] for symbol in exchange_info['symbols']
                           if symbol['contractType'] == 'PERPETUAL' and symbol['quoteAsset'] == 'USDT']
            logging.info(f"Найдено {len(all_symbols)} USDT фьючерсных пар")

            # Заполняем очередь символами
            for symbol in all_symbols:
                symbol_queue.put(symbol)

            threads = []
            NUM_THREADS = 4
            for _ in range(NUM_THREADS):
                thread = threading.Thread(target=worker, args=(client, symbol_queue, all_data))
                threads.append(thread)
                thread.start()

            # Ожидание завершения очереди
            symbol_queue.join()

            # Остановка потоков
            for _ in range(NUM_THREADS):
                symbol_queue.put(None)
            for thread in threads:
                thread.join()

            # Сохранение в JSON файл
            with open("binance_usdt_futures_volume.json", "w", encoding="utf-8") as f:
                json.dump(all_data, f, indent=4)

            logging.info("Данные успешно сохранены в файл binance_usdt_futures_volume.json")
        else:
            logging.error("Не удалось получить exchange info")
    except BinanceAPIException as e:
        logging.error(f"Ошибка API Binance: {e}")
    except Exception as e:
        logging.error(f"Непредвиденная ошибка: {e}")


def main():
    get_usdt_futures_volume_json()


if __name__ == '__main__':
    main()
