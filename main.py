"""
This class implements the main function of the classifier. Provides an Command Line Interface to trigger the
different functionalities of the program

Created on Aug 31 2018

@author: Olga Herranz Macias

"""

from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
import sys
import re

from config import get_db_connection
from database_generator.doc_parser import start_text_extraction
from database_generator.bid_crawler import get_urls_to_crawl
from database_generator.bid_crawler import start_crawl
from database_generator.doc_parser import doc_to_tokens
from database_generator.db_helpers import get_all_table_names
from database_generator.db_helpers import get_mandatory_keys
from database_generator.db_helpers import get_primary_key
# from topic_model.model_generator import model_generator
from database_generator.db_helpers import remove_duplicates
from config import db_logger
from config import split_array

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
    db_logger.debug('Database updated')
    remove_duplicates()
    # Start document analysis
    extract_text_from_docs()


def extract_text_from_docs():
    """Function to trigger document download and text extraction and storage in database

    :return:
    """
    db_conn = get_db_connection()
    num_processes = 1
    # Outer join of tables docs and texts to get unprocessed documents by their url
    # Since MySQL does not support outer join, emulate it the left join union right join
    select_qy = f"select docs.doc_url from docs left join texts on docs.doc_url=texts.doc_url where " \
        f"docs.doc_type='tecnico'"
    df = db_conn.custom_select_query(select_qy)
    urls = df['doc_url'].tolist()
    urls = split_array(urls, len(urls) // num_processes)
    p = Pool(num_processes)
    p.map(start_text_extraction, urls)
    print('Finished parsing docs!')


def reconstruct_database():
    db_conn = get_db_connection()
    mandatory_fields = list()
    primary_keys = list()
    tables = get_all_table_names()
    for table in tables:
        mandatory_fields.append(get_mandatory_keys(table))
        primary_keys.append(get_primary_key(table))
    for index, table in enumerate(tables):
        db_conn.deleteDBtables(table)
        db_conn.createDBtable(table, mandatory_fields[index], primary_keys[index])
    update_database()


def generate_model():
    # model_generator()
    pass


if __name__ == '__main__':
    start_main_menu()
