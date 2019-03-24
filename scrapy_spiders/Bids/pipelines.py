# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import logging
from logging import FileHandler
from logging import Formatter
import os
import re

from pandas import DataFrame

from config import get_db_connection


class BidsPipeline(object):
    MY_LOG_FORMAT = ("%(asctime)s %(levelname)s %(message)s")
    my_logger = logging.getLogger(__name__)
    my_logger.setLevel(logging.DEBUG)
    my_logger_file_handler = FileHandler('scrapy_spiders/Bids/log/bids_spider.log')
    my_logger_file_handler.setLevel(logging.DEBUG)
    my_logger_file_handler.setFormatter(Formatter(MY_LOG_FORMAT))
    my_logger.addHandler(my_logger_file_handler)
    spider_id = str()

    def process_item(self, item, spider):
        item = item['metadata']
        spider_id = item['spider_id']
        del item['spider_id']
        processed_item = dict()
        for field in item:
            if item[field] is not None:
                if type(item[field]) is str:
                    if item[field] == 'Euros' or item[field] == 'Ver detalle de la adjudicación':
                        processed_item[re.search('(^[^.:]*)', field).group(1).replace(" ", "_")] = [0.0]
                    else:
                        processed_item[re.search('(^[^.:]*)', field).group(1).replace(" ", "_")] = [
                            re.sub('\s+', ' ', str(item[field]))]
                else:
                    processed_item[re.search('(^[^.:]*)', field).group(1).replace(" ", "_")] = [item[field]]
        try:
            get_db_connection().upsert('bids', 'Expediente_Licitacion', DataFrame(processed_item))
            BidsPipeline.my_logger.debug(
                f'Process-{spider_id}. Bid {processed_item["Expediente_Licitacion"]} stored in database')
        except BaseException as e:
            BidsPipeline.my_logger.error(
                f'Process-{spider_id}. Error storing bid {processed_item["Expediente_Licitacion"]} in database: {str(e)}')
        return processed_item

    def close_spider(self, BidsSpider):
        fpath = os.getcwd()
        BidsPipeline.my_logger.debug(f'Process-{BidsPipeline.spider_id}. Closing spider...')
        # get_db_connection(
        # ).exportTable('bids', 'xlsx',
        # fpath,
        # 'Bids')  #
        # print(
        # 'Table "Bids" successfully updated')
