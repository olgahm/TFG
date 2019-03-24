#! /usr/bin/env python3
# -*- coding: utf-8 -*-

from logging import FileHandler
from logging import Formatter
import logging
import re

from pandas import DataFrame
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options
from unidecode import unidecode

from config import get_db_connection

MY_LOG_FORMAT = ("%(asctime)s %(levelname)s %(message)s")
my_logger = logging.getLogger(__name__)
my_logger.setLevel(logging.DEBUG)
my_logger_file_handler = FileHandler('selenium_crawler.log')
my_logger_file_handler.setLevel(logging.DEBUG)
my_logger_file_handler.setFormatter(Formatter(MY_LOG_FORMAT))
my_logger.addHandler(my_logger_file_handler)


def get_urls_scrapy(process_id, stored_profiles, total_processes, queue):
    my_logger.debug(f'[Selenium_Profiles] Process-{process_id}. Starting selenium webdriver...')
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(chrome_options=options)
    driver.implicitly_wait(10)
    process_id = process_id
    scrapy_urls = list()
    url_profiles_names = list()

    driver.get('https://contrataciondelestado.es/wps/portal/perfilContratante')
    driver.find_element_by_xpath("//input[@type='submit' and contains(@id, 'botonbuscar')]").click()

    total_pages = int(
        driver.find_element_by_id("viewns_Z7_AVEQAI930GRPE02BR764FO30G0_:listaperfiles:textTotalPagina").text)

    my_pages = get_pages(total_pages, total_processes, process_id)

    current_page = int(driver.find_element_by_id("viewns_Z7_AVEQAI930GRPE02BR764FO30G0_:listaperfiles:textNumPag").text)

    while current_page <= total_pages:
        if current_page not in my_pages:
            driver.find_element_by_xpath("//input[@type='submit' and contains(@id, 'Siguiente')]").click()
            my_logger.debug(f'[Selenium_Profiles] Process-{process_id}. Page {current_page} not to be parsed by this '
                            f'process. Going to next page...')
            current_page = int(
                driver.find_element_by_id("viewns_Z7_AVEQAI930GRPE02BR764FO30G0_:listaperfiles:textNumPag").text)
            continue
        profiles_in_current_page_wb = driver.find_elements_by_xpath(
            '(//td[@class = "tdOrganoContratacionBusqPerfil"]/a)')
        profile_name_in_current_page = list()
        unstored_profiles = list()

        for profile in profiles_in_current_page_wb:
            profile_name_in_current_page.append(re.sub('\s+', ' ', profile.text).strip())

        for profile in profile_name_in_current_page:  ## Check if the profile is stored
            if unidecode(profile).title() not in stored_profiles:
                link_number = profile_name_in_current_page.index(profile) + 1
                unstored_profiles.append(link_number)

        for index in unstored_profiles:
            profile_selenium = driver.find_element_by_xpath(
                '(//td[@class = "tdOrganoContratacionBusqPerfil"]/a)[%s]' % index)  ## Get corresponding
            # profile
            profile_name = unidecode(re.sub('\s+', ' ', profile_selenium.text).strip()).title()
            profile_selenium.click()
            driver.find_element_by_xpath("//input[contains(@id, 'linkPrepLic')]").click()  # Go to bids section
            bids_in_current_page_wd = driver.find_elements_by_xpath('(//td[@class = "tdExpediente"]/a)')
            num_bids = len(bids_in_current_page_wd)

            if num_bids > 0:  # Store profile if it has any bids
                my_logger.debug('Inspecting %s...', profile_name)
                driver.find_element_by_xpath(
                    '//*[@id="viewns_Z7_AVEQAI930GRPE02BR764FO30G0_:form1:linkPerfilComprador"]').click()
                static_url = driver.find_element_by_xpath('//a[contains(@id, "URLgenera")]').get_attribute('href')
                scrapy_urls.append(static_url)
                url_profiles_names.append(profile_name)
                # If X links have been obtained, pass list to main thread
                if len(scrapy_urls) == total_processes * 10:
                    profiles_to_scrapy = [scrapy_urls, url_profiles_names]
                    my_logger.debug(f'[Selenium_Bids] Process-{process_id}. Passing list of profiles to scrapy...')
                    queue.put(profiles_to_scrapy)
                    scrapy_urls = list()
                    url_profiles_names = list()
            driver.find_element_by_xpath("//input[contains(@id, 'idBotonVolver')]").click()  # Back to profiles' list

        if current_page != total_pages:
            driver.find_element_by_xpath("//input[@type='submit' and contains(@id, 'Siguiente')]").click()
            my_logger.debug(f'[Selenium_Profiles] Process-{process_id}. Going to next page...')
            current_page = int(
                driver.find_element_by_id("viewns_Z7_AVEQAI930GRPE02BR764FO30G0_:listaperfiles:textNumPag").text)
    else:
        my_logger.debug(f'[Selenium_Profiles] Process-{process_id}. Finished browsing, closing webdriver...')
        driver.close()
        queue.put(None)


def get_pages(total_pages, total_processes, process_id):
    pages = list()
    next_value = process_id
    while next_value <= total_pages:
        pages.append(next_value)
        next_value += total_processes
    return pages

