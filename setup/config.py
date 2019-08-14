#-*- coding: utf-8 -*-
from logging import FileHandler
from logging import Formatter
import logging
import json
import os

# Init loggers

LOG_FORMAT = ("%(asctime)s %(levelname)s %(message)s")
db_logger = logging.getLogger('db_logger')
db_logger.setLevel(logging.DEBUG)
db_logger_file_handler = FileHandler('log/db_generator.log')
db_logger_file_handler.setLevel(logging.DEBUG)
db_logger_file_handler.setFormatter(Formatter(LOG_FORMAT))
db_logger.addHandler(db_logger_file_handler)

text_logger = logging.getLogger('text_logger')
text_logger.setLevel(logging.DEBUG)
text_logger_file_handler = FileHandler('log/text_extractor.log')
text_logger_file_handler.setLevel(logging.DEBUG)
text_logger_file_handler.setFormatter(Formatter(LOG_FORMAT))
text_logger.addHandler(text_logger_file_handler)

# Load config file
with open('setup/config.json', 'r') as config_file:
    config = json.loads(config_file.read())
FILE_EXTENSIONS_TO_PARSE = config['TEXT_EXTRACTION']['formats_to_parse']
FILE_EXTENSIONS_TO_IGNORE = config['TEXT_EXTRACTION']['formats_to_ignore']
FINAL_CONTENT_TYPES_TO_PARSE = config['TEXT_EXTRACTION']['content_types_to_parse']
FINAL_CONTENT_TYPES_TO_IGNORE = config['TEXT_EXTRACTION']['content_types_to_ignore']
IRRELEVANT_STRINGS = config['TOPIC_MODELING']['irrelevant']
CUSTOM_STOPWORDS = config['TOPIC_MODELING']['custom_stopwords']
EQUIVALENCES = config['TOPIC_MODELING']['equivalences']
MALLET_BINARY_PATH = config['TOPIC_MODELING']['mallet_binary_path']
DB_TABLE_STRUCTURE = config['DB_GENERATION']['db_table_structure']
DB_CONNECTION_PARAMS = config['DB_GENERATION']['db_connection_params']
