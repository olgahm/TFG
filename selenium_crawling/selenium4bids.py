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


def get_urls_scrapy(process_id, stored_bids, total_processes, queue):
    my_logger.debug(f'[Selenium_Bids] Process-{process_id}. Starting selenium webdriver...')
    options = Options()
    options.headless = True
    driver = webdriver.Chrome(chrome_options=options)
    driver.implicitly_wait(10)
    process_id = process_id
    scrapy_urls = list()
    url_bid_name = list()
    driver.get('https://contrataciondelestado.es/wps/portal/licitaciones')  # Into bids section
    driver.find_element_by_xpath("//a[contains(@id, 'linkFormularioBusqueda')]").click()  # Al clickear en el boton
    driver.find_element_by_xpath(
        "//input[@type='submit' and contains(@id, 'button1')]").click()  # Click search buttoon to get all bids

    total_pages = int(
        driver.find_element_by_id("viewns_Z7_AVEQAI930OBRD02JPMTPG21004_:form1:textfooterInfoTotalPaginaMAQ").text)
    my_pages = get_pages(total_pages, total_processes, process_id)

    current_page = int(
        driver.find_element_by_id("viewns_Z7_AVEQAI930OBRD02JPMTPG21004_:form1:textfooterInfoNumPagMAQ").text)

    while current_page <= total_pages:
        if current_page not in my_pages:
            # if current_page % 10 == 0:
            #     yield None  # If we passed 10 pages, go to next spider to do the same
            driver.find_element_by_xpath("//input[@type='submit' and contains(@id, 'Siguiente')]").click()
            my_logger.debug(f'[Selenium_Bids] Process-{process_id}. Page {current_page} not to be parsed by this '
                            f'process. Going to next page...')
            current_page = int(
                driver.find_element_by_id("viewns_Z7_AVEQAI930OBRD02JPMTPG21004_:form1:textfooterInfoNumPagMAQ").text)
            continue
        unstored_bids = list()
        bids_in_current_page_wd = driver.find_elements_by_xpath('(//td[@class = "tdExpediente"])')

        bid_id_in_this_page = list()  # Get list of bid names in this page
        bid_object_in_this_page = list()
        for bid in bids_in_current_page_wd:  # Make bid name clearer by deleting heading and trailing tabs
            # and transforming multiple tabs in   # single ones

            bid_id_in_this_page.append(unidecode(re.sub('\s+', ' ', bid.text.split('\n')[0])).strip())
            bid_object_in_this_page.append(unidecode(re.sub('\s+', ' ', bid.text.split('\n')[1])).strip())

        # Iterate through list of stored bids and construct a list with the position of to-store bids in the
        # webpage for later crawling
        for index, bid in enumerate(bid_id_in_this_page):
            if bid not in stored_bids:
                unstored_bids.append(index + 1)
        for index in unstored_bids:
            # Find specific bid and store metadata in form in case the page cannot load
            bid_selenium = driver.find_element_by_xpath(f'(//td[@class = "tdExpediente"]/div/a)[ {index} ]')
            bid_name = bid_id_in_this_page[index - 1]
            bid_objeto_del_contrato = bid_object_in_this_page[index - 1]
            bid_tipo_de_contrato = driver.find_element_by_xpath(f'(//td[@class = "tdTipoContrato"])[{index}]').text
            bid_estado = driver.find_element_by_xpath(f'(//td[@class = "tdEstado"])[{index}]').text
            bid_importe = driver.find_element_by_xpath(f'(//td[@class = "tdImporte textAlignRight"])[{index}]').text
            bid_organo_contratacion = driver.find_element_by_xpath(
                f'(//td[@class = "tdOrganoContratacion"])[{index}]').text
            bid_metadata = {'Expediente Licitacion': bid_name, 'Organo De Contratacion': bid_organo_contratacion,
                            'Estado De La Licitacion': bid_estado, 'Objeto Del Contrato': bid_objeto_del_contrato,
                            'Tipo De Contrato': bid_tipo_de_contrato, 'Presupuesto Base De Licitación': bid_importe}

            bid_selenium.click()  # Go to bid page
            try:
                static_url = driver.find_element_by_xpath('//a[contains(@id, "URLgenera")]').get_attribute('href')
                my_logger.debug(
                    f'[Selenium_Bids] Process-{process_id}. Inspecting {bid_name} (page {current_page}, bid number '
                    f'{index}). Obtaining metadata...')
                scrapy_urls.append(static_url)
                url_bid_name.append(bid_name)
                # If X links have been obtained, pass list to main thread
                if len(scrapy_urls) == 100:
                    bids_to_scrapy = [scrapy_urls, url_bid_name]
                    my_logger.debug(f'[Selenium_Bids] Process-{process_id}. Passing list of bids to scrapy...')
                    queue.put(bids_to_scrapy)
                    scrapy_urls = list()
                    url_bid_name = list()
            except NoSuchElementException:
                my_logger.debug(
                    f'[Selenium_Bids] Process-{process_id}. Static URL to bid {bid_name} not found. Storing basic '
                    f'info...')
                store_basic_bid_info(bid_metadata)
            # Back to profile bids
            driver.find_element_by_id('enlace_volver').click()  # Back to profiles' list

        if current_page != total_pages:
            driver.find_element_by_xpath("//input[@type='submit' and contains(@id, 'Siguiente')]").click()
            my_logger.debug(f'[Selenium_Bids] Process-{process_id}. Going to next page...')
            current_page = int(
                driver.find_element_by_id("viewns_Z7_AVEQAI930OBRD02JPMTPG21004_:form1:textfooterInfoNumPagMAQ").text)
    else:
        my_logger.debug(f'[Selenium_Bids] Process-{process_id}. Finished browsing, closing webdriver...')
        driver.close()
        queue.put(None)


def get_pages(total_pages, total_processes, process_id):
    pages = list()
    next_value = process_id
    while next_value <= total_pages:
        pages.append(next_value)
        next_value += total_processes
    return pages


def store_basic_bid_info(bid_metadata, process_id):
    processed_item = dict()
    for field in bid_metadata:
        if field == 'Presupuesto Base De Licitacion':
            try:
                bid_metadata[field] = float(bid_metadata[field].replace(',', ''))
            except:
                bid_metadata[field] = 0.0
        processed_item[field.title().replace(' ', '_')] = [bid_metadata[field].strip()]
    try:
        get_db_connection().upsert('bids', 'Expediente_Licitacion', DataFrame(processed_item))
        my_logger.debug(
            f'[Selenium_Bids] Process-{process_id}. Bid {processed_item["Expediente_Licitacion"]} stored in database')

    except BaseException as e:
        my_logger.error(f'[Selenium_Bids] Process-{process_id}. Error storing bid '
                        f'{processed_item["Expediente_Licitacion"]} in database: {str(e)}')
