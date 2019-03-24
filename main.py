"""
This class implements the main function of the categorizer:

* Browse though contractor profile and construct the database
* storing calculated values in the dataset

Created on Aug 31 2018

@author: Olga Herranz Macias

"""
from multiprocessing import Process, Manager, Queue
import sys

from scrapy.crawler import CrawlerProcess

from config import get_db_connection
from config import split_array
from database_generator.info_storage import get_all_table_names
from database_generator.info_storage import update_data
from database_generator.zip_atom_extractor import zip_processing
from web_scraping.spiders.bids_spider import BidsSpider
from database_generator import info_storage
from toppic_modelling.model_generator import model_generator

scrapy_spiders = ['ProfilesSpider', 'BidsSpider', 'DocsSpider']

actions = {'1': 'update_database()', '2': 'extract_text_from_docs()', '3': 'reconstruct_database()',
           '4': 'generate_model()', '0': 'exit()'}


# Show Main menu
def start_main_menu():
    print("Welcome,\n")
    while (True):
        print("Choose one of the following actions:")
        print("1. Fill database")
        print("2. Extract text from documents")
        print("3. [Re]Construct database from scratch")
        print("4. Generate topic model")
        print("\n0. Exit")
        option = input(" >>  ")
        # option = "1"
        parse_option(option)
    return


def parse_option(option):
    if option in actions:
        eval(actions[option])
    else:
        print("Invalid option, please try again")
        start_main_menu()


def exit():
    sys.exit(0)


def extract_text_from_docs():
    # Check if there is any docs in database. If not, get info from bids
    docs_in_db = info_storage.db_stored_info['docs']
    pass

def reconstruct_database():
    mandatory_fields = list()
    primary_keys = list()
    tables = get_all_table_names()
    for table in tables:
        mandatory_fields.append(info_storage.get_mandatory_keys(table))
        primary_keys.append(info_storage.get_primary_key(table))
    for index, table in enumerate(tables):
        get_db_connection().deleteDBtables(table)
        get_db_connection().createDBtable(table, mandatory_fields[index], primary_keys[index])
    update_database()


def generate_model():
    model_generator()


def update_database():
    manager = Manager().list()
    scraped_urls = dict()
    manager.append(update_data())
    manager.append(scraped_urls)
    # info_storage.update_data()
    q = Queue()
    processes = list()
    # Run process for scaping this month's bids
    p = Process(target=run_bids_spider, args=(q, manager))
    processes.append(p)
    p.start()
    zips = list()
    # Get list of zips and start processing it in another process
    for i in range(2):
        zips += q.get()

    num_processes = 2
    split_zips = split_array(zips, len(zips) // num_processes)
    for chunk in split_zips:
        p = Process(target=zip_processing, args=(chunk, manager))
        processes.append(p)
        p.start()
    for proc in processes:
        proc.join()

    print('stopped: check the items in the manager')


def run_bids_spider(q, manager):
    process = CrawlerProcess(
        ({'USER_AGENT': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}))
    process.crawl(BidsSpider, True, manager, q)
    process.crawl(BidsSpider, False, manager, q)
    process.start()


if __name__ == '__main__':
    start_main_menu()
