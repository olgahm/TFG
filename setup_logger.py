# 05/06/2019

from logging import FileHandler
from logging import Formatter
import logging

# Init loggers for whole project

LOG_FORMAT = ("%(asctime)s %(levelname)s %(message)s")

pcsp_crawler_logger = logging.getLogger('pcsp_crawler_logger')
pcsp_crawler_logger.setLevel(logging.DEBUG)
pcsp_crawler_logger.addHandler(FileHandler('log/pcsp_crawler.log').setFormatter(Formatter(LOG_FORMAT)))

text_extractor_logger = logging.getLogger('text_extractor_logger')
text_extractor_logger.setLevel(logging.DEBUG)
text_extractor_logger.addHandler(FileHandler('log/text_extractor.log').setFormatter(Formatter(LOG_FORMAT)))


