from mysql_helpers import BaseDMsql
import logging
from logging import FileHandler
from logging import Formatter

# TODO: Funciones para procesar un JSON de entrada
db_connection = BaseDMsql(db_name='contratacion_del_estado', db_connector='mysql', db_server='localhost',
                          db_user='root', db_password='23091996')

# Init log file for database generation

LOG_FORMAT = ("%(asctime)s %(levelname)s %(message)s")
db_logger = logging.getLogger(__name__)
db_logger.setLevel(logging.DEBUG)
db_logger_file_handler = FileHandler('log/db_generator.log')
db_logger_file_handler.setLevel(logging.DEBUG)
db_logger_file_handler.setFormatter(Formatter(LOG_FORMAT))
db_logger.addHandler(db_logger_file_handler)

text_logger = logging.getLogger(__name__)
text_logger.setLevel(logging.DEBUG)
text_logger_file_handler = FileHandler('log/text_extractor.log')
text_logger_file_handler.setLevel(logging.DEBUG)
text_logger_file_handler.setFormatter(Formatter(LOG_FORMAT))
text_logger.addHandler(text_logger_file_handler)


def get_db_connection():
    """Init database connection

    :return: Database connection object
    """
    return db_connection


def split_array(arr, size):
    arrs = []
    while len(arr) > size:
        pice = arr[:size]
        arrs.append(pice)
        arr = arr[size:]
    arrs.append(arr)
    return arrs
