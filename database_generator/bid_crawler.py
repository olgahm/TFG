import requests
from io import BytesIO
import zipfile
from database_generator.atom_parser import process_xml_atom
from database_generator.atom_parser import get_next_link
from database_generator.info_storage import DB_GEN_PATH
from database_generator.atom_parser import clean_elements
from database_generator.info_storage import get_data_from_table
from config import db_logger
from lxml import etree
from datetime import datetime
import re
from bs4 import BeautifulSoup
import os


def get_urls_to_crawl():
    """Function to extract all urls to scrape

    :return: urls for historic bids and current ones
    """
    main_site = requests.get('http://www.hacienda.gob.es/es-ES/GobiernoAbierto/Datos%20Abiertos/Paginas'
                             '/licitaciones_plataforma_contratacion.aspx')
    soup = BeautifulSoup(main_site.content, 'html.parser')
    historic_atom_files = [link['href'] for link in soup.select('a[href*="zip"]')]
    current_atom_files = [link['href'] for link in soup.select('a[href*="atom"]')]

    # Check if any of the historic bids have already been processed and stored in database
    unprocessed_historic_files = list()
    if os.path.exists(os.path.join(DB_GEN_PATH, 'processed_zips.txt')):
        with open(os.path.join(DB_GEN_PATH, 'processed_zips.txt')) as f:
            processed_zips = [url.strip() for url in f.readlines()]
            for url in historic_atom_files:
                if url not in processed_zips:
                    unprocessed_historic_files.append(url)
    else:
        unprocessed_historic_files = historic_atom_files
    return sorted(unprocessed_historic_files, reverse=True) + current_atom_files


def process_url(url):
    if '.zip' in url:
        zip_processing(url)
    else:
        atom_url_processing(url)


def zip_processing(url):
    db_logger.debug(f'Start processing zip file {url}')
    response = requests.get(url)
    data = {'bids': get_data_from_table('bids'), 'orgs': get_data_from_table('orgs')}
    gc_info = dict()
    with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
        for zipinfo in zip_file.infolist():
            with zip_file.open(zipinfo) as atom_file:
                bids_xml = etree.parse(atom_file)
                root = clean_elements(bids_xml.getroot())
                pseudo_manager = [data, gc_info]
                process_xml_atom(root, pseudo_manager)
    db_logger.debug(f'Finished processing zip file {url}')
    with open(os.path.join(DB_GEN_PATH, 'processed_zips.txt'), 'a') as f:
        f.write(f'{url}\n')


def atom_url_processing(url, pseudo_manager=None):
    """Function to get bids for current month

    :param url: URL to scrape
    :return:
    """
    while True:
        try:
            atom = requests.get(url)
            break
        except BaseException as e:
            db_logger.debug(f'Exception {e} caught when trying to access url. Retrying...')
            sleep(30)
    root = etree.fromstring(atom.content)
    next_link, root = get_next_link(root)
    # Set condition to stop crawling. If the atom references last month, don't scrape since it is going to be
    # processed as historic atom file
    this_month = datetime.now().month
    next_atom_date = re.search('_(\d{8})_{0,1}', next_link)
    if pseudo_manager is None:
        pseudo_manager = [{'bids': get_data_from_table('bids'), 'orgs': get_data_from_table('orgs')}, dict()]
    process_xml_atom(root, pseudo_manager)
    if next_atom_date is not None:
        if int(next_atom_date.group(1)[4:6]) == this_month:
            atom_url_processing(next_link, pseudo_manager)
