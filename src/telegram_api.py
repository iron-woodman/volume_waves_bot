## -*- coding: utf-8 -*-
import requests
from time import sleep
import src.logger as custom_logging


def send_signal(signal, tlg_token, tlg_channel_id):
    print("*" * 30 + "\n" + signal)
    url = "https://api.telegram.org/bot"
    url += tlg_token
    method = url + "/sendMessage"
    attemts_count = 5
    while (attemts_count > 0):
        r = requests.post(method, data={
            "chat_id": tlg_channel_id,
            "text": signal,
            "parse_mode": "Markdown"
        })
        if r.status_code == 200:
            return
        elif r.status_code != 200:
            print(f'Telegram send signal error ({signal}). Status code={r.status_code}. Text="{r.text}".')
            custom_logging.error(f'Telegram send signal error:\n ({signal}). \nAttempts count={attemts_count}')
            sleep(1)
            attemts_count -= 1



def list_to_string(lst):
    mess = ''
    for item in lst:
        mess += '\n' + item + '\n'
    return mess
