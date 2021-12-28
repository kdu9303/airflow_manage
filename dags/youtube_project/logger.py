# -*- coding: utf-8 -*-


import logging
from logging.handlers import TimedRotatingFileHandler
import sys

"""
main파일안에  log = logger.setup_applevel_logger(file_name = 'app.log')로 초기화시킨다

"""

APP_LOGGER_NAME = 'Youtube_Project'


def setup_applevel_logger(logger_name=APP_LOGGER_NAME, file_name=None):

    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)

    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    formatter = logging.Formatter(log_format)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(formatter)

    logger.handlers.clear()
    logger.addHandler(sh)

    if file_name:
        # fh = logging.FileHandler(file_name)
        log_path = "./logs/"
        fh = TimedRotatingFileHandler(log_path+file_name,
                                      when="midnight",
                                      interval=1)
        fh.suffix = "%Y_%m_%d"
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger


def get_logger(module_name):
    return logging.getLogger(APP_LOGGER_NAME).getChild(module_name)
