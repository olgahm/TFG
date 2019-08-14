#-*- coding: utf-8 -*-

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

from helpers.helpers import get_db_connection, split_array, remove_duplicates
from text_extractor.text_extractor import start_text_extraction
from database_generator.bid_crawler import get_urls_to_crawl
from database_generator.bid_crawler import start_crawl
from topic_modeler.model_generator import train_model 
from setup.config import db_logger
from setup.config import DB_TABLE_STRUCTURE

from threading import Thread
from threading import Lock
from copy import deepcopy

cpvs4docs = dict()
cpvs4texts = dict()
db_conn = get_db_connection()


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


def extract_text_from_docs():
    """Function to trigger document download and text extraction and storage in database

    :return:
    """
    select_qy = "select code, code_description from bid_cpv_codes inner join docs on bid_cpv_codes.bid_id = docs.bid_id WHERE docs.doc_type = 'tecnico' AND docs.idioma IS NULL group by code, code_description"
    chosen_code, path = create_cpv_code_menu(select_qy)
    if not path:
        return
    # print(chosen_code)
    # print(f'Extracting text from documents related to CPV code {chosen_code}...')
    select_qy = f"SELECT distinct doc_url FROM docs WHERE docs.doc_type='tecnico' AND docs.idioma is NULL"
    df = db_conn.custom_select_query(select_qy)
    doc_urls = df['doc_url'].tolist()
    select_qy = f"SELECT distinct doc_url FROM texts"
    df = db_conn.custom_select_query(select_qy)
    text_urls = df['doc_url'].tolist()
    
    urls = [url for url in doc_urls if url not in text_urls]


    # print('Obtaining doc urls for chosen cpv code')
    if len(chosen_code) == 8:
        select_qy = f"SELECT distinct doc_url FROM docs INNER JOIN bid_cpv_codes ON docs.bid_id = bid_cpv_codes.bid_id WHERE code = '{chosen_code}'"
        df = db_conn.custom_select_query(select_qy)
        filtered_urls = df['doc_url'].tolist()
        urls = [url for url in urls if url in filtered_urls]
        print(len(urls))
    elif len(chosen_code) > 0:
        select_qy = f"SELECT distinct doc_url FROM docs INNER JOIN bid_cpv_codes ON docs.bid_id = bid_cpv_codes.bid_id WHERE code LIKE '{chosen_code}%'"
        df = db_conn.custom_select_query(select_qy)
        filtered_urls = df['doc_url'].tolist()
        urls = [url for url in urls if url in filtered_urls]
    # print('Finished queries')
    # urls = split_array(urls, len(urls) // num_processes)
    # for url in urls:
    start_text_extraction(urls)
    # p = Pool(num_processes)
    # p.map(start_text_extraction, urls)
    # print('Finished parsing docs!')


def generate_model():
    print('Insert the number of topics for the model:')
    while True:
        option = input(" >> ")
        try:
            option = int(option)
            break
        except:
            print('Insert a valid number of topics')
    train_model(option)


def reconstruct_database():
    """Function to reconstruct database from scratch

    :return:
    """
    global db_conn
    # Get current info and delete all tables
    tablenames = db_conn.getTableNames()
    for table in tablenames:
        db_conn.deleteDBtables(table)
    # Prepare table structure
    tables = [table for table in DB_TABLE_STRUCTURE]
    for table in tables:
        mandatory_fields = DB_TABLE_STRUCTURE[table]['fields']
        primary_key = DB_TABLE_STRUCTURE[table]['primary_key']
        db_conn.createDBtable(table, mandatory_fields, primary_key)
    update_database()


def create_cpv_code_menu(select_qy):
    global db_conn
    global cpvs4docs
    if not cpvs4docs:
        cpvs4docs = create_cpv_tree(db_conn, select_qy)
    path = list()
    shown_cpvs = cpvs4docs
    chosen_code = str()
    # Once we have created the dict, we need a menu for nevgating through all codes:
    while True:
        submenu = dict()
        print('Choose one of the following CPV codes:')
        for index, code in enumerate(shown_cpvs):
            print(f'{index + 1}. {code}\t{shown_cpvs[code]["desc"]}')
            submenu[index + 1] = code
        print(f'{index + 2}. Use all shown CPV codes')
        print('\n0. Back')
        while True:
            option = input(" >> ")
            if option:
                option = int(option)
                break
        if option == 0 and path:
            del path[-1]
            print(path)
            shown_cpvs = cpv_list
            for code_set in path:
                shown_cpvs = shown_cpvs[code_set]['inner']
        elif option == len(shown_cpvs.keys()) + 1 or option == 0:
            break
        else:
            path.append(submenu[option])
            chosen_code = submenu[option].rstrip('0')
            if 'inner' in shown_cpvs[submenu[option]]:
                # if len(shown_cpvs[submenu[option]]['inner'].keys()) > 1:
                #     shown_cpvs = shown_cpvs[submenu[option]]['inner']
                # else:
                #     # If there is only one subcode, just take the code without showing a further menu
                #     chosen_code = list(shown_cpvs[submenu[option]]['inner'].keys())[0]
                #     break
                shown_cpvs = shown_cpvs[submenu[option]]['inner']

            else:
                # If the current code has not sub codes, then take it
                chosen_code = submenu[option]
                break
    return chosen_code, path


def create_cpv_tree(db_conn, select_qy):
    df = db_conn.custom_select_query(select_qy)
    codes = df['code'].tolist()
    code_descriptions = df['code_description'].tolist()
    cpv_dict = dict()
    code_prefixes = [f'{code[:2]}{code[2:].replace("0", "")}' for code in codes]

    subcode2uppercode_map = dict()
    for code in codes:
        prefix = f'{code[:2]}{code[2:].replace("0", "")}'
        upper_prefix = prefix[:-1]
        while len(upper_prefix) > 1:
            if upper_prefix in code_prefixes:
                subcode2uppercode_map[code] = (upper_prefix + '00000000')[:8]
                break
            else:
                upper_prefix = upper_prefix[:-1]
        else:
            subcode2uppercode_map[code] = None

    for index, code in enumerate(codes):
        desc = re.sub('\s+', ' ', code_descriptions[index])
        if subcode2uppercode_map[code] is None:
            cpv_dict[code] = {'desc': desc,
                              'inner': find_sub_codes(code, subcode2uppercode_map, codes, code_descriptions)}

    return cpv_dict


def find_sub_codes(upper_code, subcode2uppercode_map, codes, descs):
    subcodes4code = [code for code in subcode2uppercode_map if subcode2uppercode_map[code] == upper_code]
    subcode_dict = dict()
    for code in subcodes4code:
        index = codes.index(code)
        desc = re.sub('\s+', ' ', descs[index])
        code_dict = {'desc': desc, 'inner': find_sub_codes(code, subcode2uppercode_map, codes, descs)}
        if not code_dict['inner']:
            del code_dict['inner']
        subcode_dict[code] = code_dict
    return subcode_dict


actions = {'1': {'title': 'Re/construct database structure', 'function': reconstruct_database},
           '2': {'title': 'Update bids', 'function': update_database}, 
           '3': {'title': 'Extract text from documents', 'function': extract_text_from_docs}, 
           # '4': {'title': 'Generate topic model', 'function': generate_model}, 
           '0': {'title': 'Exit', 'function': exit}
           }


# Show Main menu
def start_main_menu():
    while (True):
        print("Choose one of the following actions:")
        for action in actions:
            if action == '0':
                print(f'\n{action}. {actions[action]["title"]}')
            else:
                print(f'{action}. {actions[action]["title"]}')
        option = input(" >>  ")
        if option in actions:
            actions[option]['function']()
        else:
            print('Invalid option')


if __name__ == '__main__':
    print("Welcome to PCSP's data processor,")
    start_main_menu()
