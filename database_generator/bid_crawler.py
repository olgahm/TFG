from io import BytesIO
import os

from bs4 import BeautifulSoup
from lxml import etree
import re
import requests
import zipfile

from config import db_logger
from database_generator.atom_parser import clean_elements
from database_generator.atom_parser import get_next_link
from database_generator.atom_parser import process_xml_atom
from database_generator.db_helpers import DB_GEN_PATH
from database_generator.db_helpers import get_data_from_table
from database_generator.db_helpers import get_db_bid_info

from config import get_db_connection
from config import db_logger
from lxml import etree
from datetime import datetime


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
    bids_pcsp = [url for url in unprocessed_historic_files if 'licitacionesPerfilesContratanteCompleto' in url]
    bids_not_pcsp = [url for url in unprocessed_historic_files if 'PlataformasAgregadasSinMenores' in url]
    minor_contracts = [url for url in unprocessed_historic_files if 'contratosMenoresPerfilesContratantes' in url]
    cur_bids_pcsp = [url for url in current_atom_files if 'licitacionesPerfilesContratanteCompleto' in url]
    cur_bids_not_pcsp = [url for url in current_atom_files if 'PlataformasAgregadasSinMenores' in url]
    cur_minor_contracts = [url for url in current_atom_files if 'contratosMenoresPerfilesContratantes' in url]

    bids_pcsp = cur_bids_pcsp + sorted(bids_pcsp, reverse=True)
    bids_not_pcsp = cur_bids_not_pcsp + sorted(bids_not_pcsp, reverse=True)
    minor_contracts = cur_minor_contracts + sorted(minor_contracts, reverse=True)
    return bids_pcsp, bids_not_pcsp, minor_contracts


def start_crawl(urls, lock):
    db_conn = get_db_connection()
    for url in urls:
        if '.zip' in url:
            parse_zip(url, db_conn, lock)
        else:
            parse_atom(url, db_conn)


def parse_zip(url, db_conn, lock):
    db_logger.debug(f'Start processing zip file {url}')
    response = requests.get(url)
    db_logger.debug('Loading bid and organization information from database...')
    data = {'bids': get_db_bid_info(), 'orgs': get_data_from_table('orgs')}
    gc_info = dict()
    with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
        for zipinfo in reversed(zip_file.infolist()):
            with zip_file.open(zipinfo) as atom_file:
                bids_xml = etree.parse(atom_file)
                root = clean_elements(bids_xml.getroot())
                pseudo_manager = [data, gc_info]
                process_xml_atom(root, db_conn, pseudo_manager)
    db_logger.debug(f'Finished processing zip file {url}')
    lock.acquire()
    with open(os.path.join(DB_GEN_PATH, 'processed_zips.txt'), 'a') as f:
        f.write(f'{url}\n')
    lock.release()


def parse_atom(url, db_conn, pseudo_manager=None):
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
            # sleep(30)
    root = etree.fromstring(atom.content)
    next_link, root = get_next_link(root)
    # Set condition to stop crawling. If the atom references last month, don't scrape since it is going to be
    # processed as historic atom file
    this_month = datetime.now().month
    next_atom_date = re.search('_(\d{8})_{0,1}', next_link)
    if pseudo_manager is None:
        db_logger.debug('Loading bid and organization information from database...')
        pseudo_manager = [{'bids': get_db_bid_info(), 'orgs': get_data_from_table('orgs')}, dict()]
    process_xml_atom(root, db_conn, pseudo_manager)
    if next_atom_date is not None:
        if int(next_atom_date.group(1)[4:6]) == this_month:
            parse_atom(next_link, db_conn, pseudo_manager)
