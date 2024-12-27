import datetime
import json
import os
import time
from src.telegram_api import send_signal
from src.config_handler import TLG_TOKEN, TLG_CHANNEL_ID


def read_signal_data(file):
    if os.path.isfile(file) is False:  # file not exists
        print(f'File  "{file}" not exists.')
        return None
    with open(file, 'r', encoding='utf-8') as f:
        signal_data = json.load(f)
    return signal_data


def load_data_from_json_file(file_path):
    """
    Функция для чтения данных из JSON файла
    """
    # Попытка открыть и прочитать файл
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            # Загрузка данных из файла
            data = json.load(file)
            return data
    except FileNotFoundError:
        print(f"Файл не найден: {file_path}")
    except json.JSONDecodeError:
        print(f"Ошибка декодирования JSON в файле: {file_path}")


def get_sorted_by_procent_list(signal_data: dict) -> list:
    """
    отсортировать значения словаря по убыванию процента изменения цены с момента открытия дневного бара
    """
    sorted_list = []
    while len(signal_data) > 0:
        signal = signal_data.popitem()
        if len(sorted_list) == 0:
            sorted_list.append(signal)
        else:
            data_inserted = False
            for index, item in enumerate(sorted_list, start=0):
                if signal[1][1] > item[1][1]:
                    sorted_list.insert(index, signal)
                    data_inserted = True
                    break
            if not data_inserted:
                sorted_list.append(signal)


    return sorted_list


def process_signal(signal_folder_name, current_date):
    # signal_data = read_signal_data(f'signals/{current_date}.txt')
    signal_data = read_signal_data(f'{signal_folder_name}/{current_date}.txt')

    if signal_data:

        for pattern in signal_data:
            signal_str = f'{pattern}:\n'
            coin_list = signal_data[pattern]
            sorted_coin_list = sorted(coin_list)
            for coin in sorted_coin_list:
                signal_str += f'{coin}\n'
            send_signal(signal_str, TLG_TOKEN, TLG_CHANNEL_ID)
            time.sleep(1)


if __name__ == "__main__":

    cur_date = datetime.date.today().isoformat()

    # process_signal('signals_2bars_falling_volumes_v1', cur_date)
    # time.sleep(1)
    # process_signal('signals_2bars_falling_volumes_v2', cur_date)
    # time.sleep(1)
    process_signal('signals_pinbar', cur_date)
    time.sleep(1)
    # process_signal('signals_3bars_growing_volumes_v2', cur_date)






