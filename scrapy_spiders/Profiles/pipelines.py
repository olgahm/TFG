# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import os
import re

from pandas import DataFrame
from config import get_db_connection
import logging
from logging import FileHandler, Formatter


class ProfilesPipeline(object):
    # Init log file
    MY_LOG_FORMAT = ("%(asctime)s %(levelname)s %(message)s")
    my_logger = logging.getLogger(__name__)
    my_logger.setLevel(logging.DEBUG)
    my_logger_file_handler = FileHandler('scrapy_spiders/Profiles/log/profiles_spider.log')
    my_logger_file_handler.setLevel(logging.DEBUG)
    my_logger_file_handler.setFormatter(Formatter(MY_LOG_FORMAT))
    my_logger.addHandler(my_logger_file_handler)

    def process_item(self, item, spider):
        item = item['metadata']
        processed_item = dict()
        for field in item:
            processed_item[(re.search('(^[^.:]*)', field).group(1).strip().replace(" ", "_"))] = [
                re.sub('\s+', ' ', str(item[field])).strip()]
        processed_item['all_bids_inspected'] = ['false']
        get_db_connection().upsert('profiles', 'Órgano_de_Contratación', DataFrame(processed_item))
        return processed_item

    def close_spider(self, ProfileSpider):
        fpath = os.getcwd()
        get_db_connection().exportTable('profiles', 'xlsx', fpath + '/DB_Tables', 'profiles')
        print('Table "Profiles" successfully updated')
