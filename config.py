import logging
from logging import FileHandler
from logging import Formatter
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
with open('config.json', 'r') as config_file:
    config = json.loads(config_file.read())
FILE_EXTENSIONS_TO_PARSE = config['formats_to_parse']
FILE_EXTENSIONS_TO_IGNORE = config['formats_to_ignore']
FINAL_CONTENT_TYPES_TO_PARSE = config['content_types_to_parse']
FINAL_CONTENT_TYPES_TO_IGNORE = config['content_types_to_ignore']
IRRELEVANT_STRINGS = config['irrelevant']
CUSTOM_STOPWORDS = config['custom_stopwords']
DB_TABLE_STRUCTURE = config['db_table_structure']
DB_CONNECTION_PARAMS = config['db_connection_params']
EQUIVALENCES = config['equivalences']
MALLET_BINARY_PATH = config['mallet_binary_path']

# Global variables

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
