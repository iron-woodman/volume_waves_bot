import json
import logging
from src.config_handler import MIN_VOLUME
# Настройка логирования
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def filter_coins_by_volume(filename: str, min_volume: float = 1_000_000.0):
    """
    Фильтрует криптовалютные пары по суточному объему торгов из JSON файла.

    Args:
        filename: Путь к JSON файлу с данными об объемах (coin: volume).
        min_volume: Минимальный суточный объем для включения в отфильтрованный список.
    Returns:
        Список кортежей вида (coin, volume) с отфильтрованными монетами или None в случае ошибки.
    """

    try:
        with open(filename, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        logging.error(f"Файл {filename} не найден.")
        return None
    except json.JSONDecodeError:
        logging.error(f"Ошибка декодирования JSON в файле {filename}.")
        return None
    except Exception as e:
        logging.error(f"Произошла непредвиденная ошибка при чтении файла: {e}")
        return None

    filtered_coins = [(coin, volume) for coin, volume in data.items() if volume >= min_volume or coin =='BTCUSDT']

    # Сортировка по объему (от большего к меньшему)
    filtered_coins.sort(key=lambda item: item[1], reverse=True)
    logging.info(f"Найдено {len(filtered_coins)} монет c объемом больше {min_volume}")

    return filtered_coins


def main():
    filename = "binance_usdt_futures_volume.json"  # Укажите путь к вашему файлу
    filtered_coins = filter_coins_by_volume(filename, MIN_VOLUME)

    if filtered_coins:
        index=1
        print("Отфильтрованные монеты (по объему, от большего к меньшему):")
        for coin, volume in filtered_coins:
            print(f"{index}\t{coin}: {volume}")
            index +=1


if __name__ == "__main__":
    main()
