#! /usr/bin/env python3
# -*- coding: utf-8 -*-

"""Scrapy spider for getting information about profiles

This module browses through the Spanish Contracting Platform and crawls the contractor profiles.

Created on May 11 2018

@author: Olga Herranz Macías

"""

from scrapy import Spider
from scrapy import Request
from unidecode import unidecode

from scrapy_spiders.Profiles.items import ProfileItem
from scrapy_spiders.Profiles.pipelines import ProfilesPipeline


class ProfilesSpider(Spider):
    name = 'profilesSpider'
    allowed_domains = ['contrataciondelestado.es']
    custom_settings = {"ITEM_PIPELINES": {'scrapy_spiders.Profiles.pipelines.ProfilesPipeline': 300},
                       "LOG_FILE": "scrapy_spiders/Profiles/log/scrapy.log", "CONCURRENT_REQUESTS": 30,
                       "CONCURRENT_REQUESTS_PER_IP": 30}

    ## Set class variables for the spider
    mandatory_fields = ['Órgano_de_Contratación', 'Contracting_organisation', 'NIF', 'Direct_link']
    primary_key = 'Órgano_de_Contratación'
    foreign_key = dict()

    def __init__(self, profile_names, urls, thread_number):
        self.profiles = profile_names
        self.urls = urls
        self.thread_number = thread_number
        ProfilesPipeline.my_logger.debug(f'Starting spider {thread_number}...')

    def start_requests(self):
        for index, profile_name in enumerate(self.profiles):
            profile_metadata = {'profile_name': profile_name}
            yield Request(url=self.urls[index], callback=self.parse_profiles,
                          meta={'profile_metadata': profile_metadata})

    # Get metadata for all profiles
    def parse_profiles(self, response):
        profile_metadata = dict()
        for row in response.selector.xpath('//ul/li[contains(@class, "atributoLicitacion")]'):
            field_info = row.xpath('./..//following-sibling::li/span/text()').extract()
            if len(field_info) < 2:
                field_name = 'Actividades'
                field_value = field_info[0]
            else:
                field_name = field_info[0]
                field_value = field_info[1]
            profile_metadata[field_name] = field_value
        profile_metadata['Órgano de Contratación:'] = unidecode(response.meta['profile_metadata']['profile_name'])
        # Create item to be processed by the Item pipeline to store it in the database
        ProfilesPipeline.my_logger.debug(f'Process-{self.thread_number}. Storing profile '
                                         f'{profile_metadata["Órgano de Contratación:"]} in database...')
        yield ProfileItem(metadata=profile_metadata)
