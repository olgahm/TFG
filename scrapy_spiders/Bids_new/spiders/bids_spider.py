#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""Scrapy spider for getting information about bids

This module implements a scrapy spider for obtaining information about bids.

Created on May 11 2018

@author: Olga Herranz Macías

"""
from scrapy import Spider
from scrapy import Request
import requests

from database_generator.atom_parser import process_xml_atom
from database_generator.info_storage import update_data
from database_generator.atom_parser import get_next_link
from scrapy_spiders.Bids_new.items import ContractingItem
import xml.etree.ElementTree
import re
from datetime import datetime


class BidsSpider(Spider):
    name = 'BidsSpider'  # Name of spider
    custom_settings = {"ITEM_PIPELINES": {'scrapy_spiders.Bids_new.pipelines.BidsPipeline': 300},
                       "LOG_FILE": 'scrapy_spiders/Bids_new/log/scrapy_bids_spider.log', "CONCURRENT_REQUESTS": 30}

    start_urls = ["http://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas"
                  "/licitaciones_plataforma_contratacion.aspx"]

    def __init__(self, hosted_flag, manager, q=None):
        self.hosted_flag = hosted_flag
        self.q = q
        self.manager = manager
        # update_data(db_data)

    def parse(self, response):
        """Function for scraping data from found static bid page

        :param response: scrapy response object
        :return:
        """
        historic_atom_files = response.selector.xpath('//a[contains(@href, "zip")]').xpath('@href').extract()
        atom_files = response.selector.xpath('//a[contains(@href, "atom")]').xpath('@href').extract()
        if self.hosted_flag:
            atom_file = [link for link in atom_files if 'Agregadas' not in link][0]
            historic_atom_files = [link for link in historic_atom_files if 'Agregadas' not in link]
        else:
            atom_file = [link for link in atom_files if 'Agregadas' in link][0]
            historic_atom_files = [link for link in historic_atom_files if 'Agregadas' in link]

        self.q.put(historic_atom_files)
        yield Request(url=atom_file, callback=self.parse_atom_bid)

    def parse_atom_bid(self, response):
        """Callback function for processing atom files and storing bid information in the database.

        :param response:
        :return:
        """

        atom = response.body

        root = xml.etree.ElementTree.fromstring(atom)
        next_link, root = get_next_link(root)
        # Set condition to stop crawling. If the atom references last month, don't scrape since it is going to be
        # processed as historic atom file
        this_month = datetime.now().month
        next_atom_date = re.search('_(\d{8})_{0,1}', next_link)
        if next_atom_date is not None:
            if int(next_atom_date.group(1)[4:6]) == this_month:
                yield Request(url=next_link, callback=self.parse_atom_bid)
        items = process_xml_atom(root, self.manager)
        for item in items:
            yield ContractingItem(metadata=item)

