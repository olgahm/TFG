"""
This class implements the main function of the classifier. Provides an Command Line Interface to trigger the
different functionalities of the program

Created on Aug 31 2018

@author: Olga Herranz Macias

"""

from multiprocessing import Pool
import sys
import re

from config import get_db_connection
from database_generator.bid_crawler import get_urls_to_crawl
from database_generator.bid_crawler import start_crawl
from database_generator.doc_parser import store_document_text
from database_generator.db_helpers import get_all_table_names
from database_generator.db_helpers import get_mandatory_keys
from database_generator.db_helpers import get_primary_key
# from topic_model.model_generator import model_generator
from database_generator.db_helpers import remove_duplicates

from threading import Thread, Lock

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
    """Function to trigger document download and text extraction and storage in database

    :return:
    """
    tech_docs_info = get_db_connection().readDBtable(tablename='docs', selectOptions='*',
                                                     filterOptions="doc_type='tecnico'")
    urls = tech_docs_info['doc_url'].tolist()
    bid_ids = tech_docs_info['bid_id'].tolist()
    doc_ids = tech_docs_info['doc_id'].tolist()
    hashes = tech_docs_info['doc_hash'].tolist()
    stored_bids = get_db_connection().readDBtable(tablename='texts', selectOptions='bid_id')['bid_id'].tolist()
    stored_bids = list(dict.fromkeys([re.sub('_\d+', '', bid) for bid in stored_bids]))
    tech_docs = list()
    for index in range(len(urls)):
        if bid_ids[index] not in stored_bids:
            tech_docs.append([urls[index], bid_ids[index], doc_ids[index], hashes[index]])
    for doc in tech_docs:
        store_document_text(doc[0], doc[1], doc[2], doc[3])
    # p = Pool(2)
    # p.starmap(store_document_text, tech_docs)
    print('Finished parsing docs!')


def reconstruct_database():
    
    mandatory_fields = list()
    primary_keys = list()
    tables = get_all_table_names()
    for table in tables:
        mandatory_fields.append(get_mandatory_keys(table))
        primary_keys.append(get_primary_key(table))
    for index, table in enumerate(tables):
        get_db_connection().deleteDBtables(table)
        get_db_connection().createDBtable(table, mandatory_fields[index], primary_keys[index])
    update_database()


def generate_model():
    # model_generator()
    pass


def update_database():
    urls_pcsp, urls_not_pcsp, urls_minor_contracts = get_urls_to_crawl()

    lock = Lock()
    t1 = Thread(target=start_crawl, args=(urls_pcsp, lock))
    t2 = Thread(target=start_crawl, args=(urls_not_pcsp, lock))
    t3 = Thread(target=start_crawl, args=(urls_minor_contracts, lock))

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()
    # pool = Pool()
    # pool.map(start_crawl, urls_pcsp + urls_minor_contracts + urls_not_pcsp)
    # for url in urls:
    #     process_url(url)
    print('Finished')
    remove_duplicates()
    # extract_text_from_docs()


if __name__ == '__main__':
    start_main_menu()
