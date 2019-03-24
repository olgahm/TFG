#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""Scrapy spider for getting information about bids

This module implements a scrapy spider for obtaining information about bids.

Created on May 11 2018

@author: Olga Herranz Macías

"""

from scrapy import Spider
from scrapy import Request
from unidecode import unidecode

from scrapy_spiders.Bids.items import BidItem
from scrapy_spiders.Bids.pipelines import BidsPipeline


class BidsSpider(Spider):
    name = 'BidsSpider'  # Name of spider
    # allowed_domains = ['contrataciondelestado.es']
    # Set settings for using the proper pipeline to process the Bid and store it in the database
    custom_settings = {"ITEM_PIPELINES": {'scrapy_spiders.Bids.pipelines.BidsPipeline': 300},
                       "LOG_FILE": 'scrapy_spiders/Bids/log/scrapy_bids_spider.log', "CONCURRENT_REQUESTS": 30,
                       "CONCURRENT_REQUESTS_PER_IP": 30}

    # Data for creating the main fields of the 'bids' table in the database
    mandatory_fields = ['Expediente_Licitacion']
    primary_key = 'Expediente_Licitacion'
    foreign_key = dict()

    # Init log file

    def __init__(self, bid_names, urls, thread_number):
        self.bids = bid_names
        self.urls = urls
        self.thread_number = thread_number
        BidsPipeline.my_logger.debug(f'Starting spider {thread_number}...')

    def start_requests(self):
        for index, bid_name in enumerate(self.bids):
            bid_metadata = {'Expediente Licitacion': bid_name}
            yield Request(url=self.urls[index], callback=self.parse_bid_metadata, meta={'bid_metadata': bid_metadata})

    def parse_bid_metadata(self, response):
        """Function for scraping data from found static bid page

        :param response: scrapy response object
        :return:
        """
        bid_name = response.meta['bid_metadata']['Expediente Licitacion']
        BidsPipeline.my_logger.debug(f'Process-{self.thread_number}. Extracting metadata for bid {bid_name}')
        bid_metadata = dict()
        for row in response.selector.xpath('//ul/li[contains(@class, "atributoLicitacion")]'):
            field_info = row.xpath('./..//following-sibling::li/span/text()').extract()
            if len(field_info) < 2:
                field_name = field_info[0].strip()
                field_value = row.xpath('./..//following-sibling::li/a/span/text()').extract_first()
            else:
                field_name = field_info[0].strip()
                field_value = field_info[1].strip()
            try:  # Check if field is a number
                field_value = int(field_value)
            except:
                try:  # Check if field is a double
                    field_value = float(field_value.replace(",", ""))
                except:
                    pass
            bid_metadata[unidecode(field_name.title())] = field_value

        bid_metadata['Expediente Licitacion'] = bid_name
        bid_metadata['spider_id'] = self.thread_number
        # Get if there is document for bid
        doc_link = response.selector.xpath('//td/div[contains(text(), "Pliego") and not (contains(text(), '
                                           '"Anulación"))]/ancestor::tr/td[contains(@class, "documentosPub")]/div/a['
                                           'text()="Html"]/@href')
        if not doc_link:
            BidsPipeline.my_logger.debug(
                f'Process-{self.thread_number}. No technical specs doc found for bid {bid_name}. Storing in '
                f'database...')
            yield BidItem(metadata=bid_metadata)
        else:
            yield Request(url=doc_link.extract_first(), callback=self.parse_mid_to_doc, meta={'metadata': bid_metadata},
                          dont_filter=True)

    def parse_mid_to_doc(self, response):
        """Function for transition URL between page with bid meta-information and the document

        :param response:
        :return:
        """
        bid_metadata = response.meta['metadata']
        bid_name = bid_metadata['Expediente Licitacion']
        BidsPipeline.my_logger.debug('Trying to obtain link to doc for bid %s', bid_name)
        doc_link = response.selector.xpath('//a[text()="Pliego Prescripciones Técnicas"]/@href')
        doc_link = doc_link.extract_first()
        if doc_link:
            yield Request(url=doc_link, callback=self.parse_doc_format, meta={'metadata': bid_metadata},
                          dont_filter=True)
            BidsPipeline.my_logger.debug(f'Process-{self.thread_number}. Found link to doc for bid {bid_name}. '
                                         f'Obtaining doc format...')
        else:
            yield BidItem(metadata=bid_metadata)
            BidsPipeline.my_logger.debug(
                f'Process-{self.thread_number}. No technical specs doc found for bid {bid_name}. Storing in '
                f'database...')

    def parse_doc_format(self, response):
        bid_metadata = response.meta['metadata']
        bid_name = bid_metadata['Expediente Licitacion']
        # We are in the hopper through the document
        file_type = str(response.headers.get("content-type", "").lower())
        if 'pdf' in file_type:
            specs_format = 'pdf'
        elif 'html' in file_type:
            specs_format = 'html'
        elif 'word' in file_type:
            specs_format = 'word'
        elif 'zip' in file_type:
            specs_format = 'zip'
        else:
            specs_format = file_type
            print(file_type)
        bid_metadata['Enlace A Pliego'] = response.url
        bid_metadata['Formato Pliego'] = specs_format
        yield BidItem(metadata=bid_metadata)
        BidsPipeline.my_logger.debug(f'Process-{self.thread_number}. Obtained document format for bid {bid_name}. '
                                     f'Storing in database...')
