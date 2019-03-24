# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging
from logging import FileHandler
from logging import Formatter
import os

from database_generator.info_storage import item_to_database


class BidsPipeline(object):
    MY_LOG_FORMAT = ("%(asctime)s %(levelname)s %(message)s")
    my_logger = logging.getLogger(__name__)
    my_logger.setLevel(logging.DEBUG)
    my_logger_file_handler = FileHandler('web_scraping/Bids/log/bids_spider.log')
    my_logger_file_handler.setLevel(logging.DEBUG)
    my_logger_file_handler.setFormatter(Formatter(MY_LOG_FORMAT))
    my_logger.addHandler(my_logger_file_handler)
    spider_id = str()

    def process_item(self, item, spider):
        item_dict = item['metadata']
        # If table name is poped, this item has already been stored in database
        if item_dict.get('table_name', ''):
            table = item_dict.pop('table_name')
            processed_item = item_to_database(item_dict, table)
            return processed_item
        else:
            print(f'malformed item: {str(item)}')

    def close_spider(self, BidsSpider):
        print('Closing spider')
        BidsPipeline.my_logger.debug(f'Closing spider...')
