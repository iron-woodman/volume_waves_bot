## -*- coding: utf-8 -*-
import logging
import os
from datetime import datetime


def setup_logging(log_file="./logs/volme_waves_bot.log", level=logging.INFO):
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)

    dir_name = os.path.dirname(log_file)
    if not os.path.exists(dir_name):
        os.makedirs(dir_name)

    handler = logging.FileHandler(filename=log_file, encoding=None)
    handler.setFormatter(formatter)


    logger = logging.getLogger()
    logger.addHandler(handler)
    logger.setLevel(level)

    #Для вывода сообщений также и в консоль раскомментируйте следующую строку:
    logging.getLogger().addHandler(logging.StreamHandler())


setup_logging()


def info(message):
    logger = logging.getLogger(__name__)
    logger.info(message)


def error(message):
    logger = logging.getLogger(__name__)
    logger.error(message)


def warning(message):
    logger = logging.getLogger(__name__)
    logger.warning(message)


