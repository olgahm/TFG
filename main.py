"""
This class implements the main function of the categorizer:

* Browse though contractor profile and construct the database
* storing calculated values in the dataset

Created on Aug 31 2018

@author: Olga Herranz Macias

"""
import multiprocessing
import sys

from scrapy.crawler import CrawlerProcess

from config import get_db_connection
from config import split_array
from database_generator.info_storage import my_logger as doc_logger
from database_generator.pdf_parser import get_pdf_text
from scrapy_spiders.Bids.spiders.bids_spider import BidsSpider
from scrapy_spiders.Bids.pipelines import BidsPipeline
from scrapy_spiders.Profiles.pipelines import ProfilesPipeline
from scrapy_spiders.Profiles.spiders.profiles_spider import ProfilesSpider
from selenium_crawling import selenium4bids
from selenium_crawling import selenium4profiles
from toppic_modelling.model_generator import model_generator

scrapy_spiders = ['ProfilesSpider', 'BidsSpider', 'DocsSpider']

actions = {'1': 'update_database()', '2': 'reconstruct_database()', '3': 'generate_model()', '0': 'exit()'}
database_name = 'contratacion_del_estado'


# Show Main menu
def start_main_menu():
    print("Welcome,\n")
    while (True):
        print("Choose one of the following actions:")
        print("1. Update database")
        print("2. [Re]Construct database from scratch")
        print("3. Generate topic model")
        print("\n0. Exit")
        option = input(" >>  ")
        # option = "2"
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

    return sorted(aux)


def reconstruct_database():
    print("Choose tables to [re]construct:")
    print("1. Profiles")
    print("2. Bids")
    print("3. Technical docs")
    print("4. Reconstruct all tables")
    print("\n0. Back to menu")
    option = int(input(" >>  "))
    if option != 0:

        tables = list()
        if option == 1:
            tables = ['ProfilesSpider']
        elif option == 2:
            tables = ['BidsSpider']
        elif option == 3:
            tables = ['DocsSpider']
        elif option == 4:
            tables = scrapy_spiders
        for spider in tables:
            table = spider.replace("Spider", "").lower()
            get_db_connection().deleteDBtables(table)
            get_db_connection().createDBtable(table, eval(spider).mandatory_fields, eval(spider).primary_key,
                                              eval(spider).foreign_key)
        update_database(update=False)


def generate_model():
    model_generator()


def update_database(update=True):
    print("Choose tables to update:")
    print("1. Profiles")
    print("2. Bids")
    print("3. Technical docs")
    print("4. Update all tables")
    print("\n0. Back to menu")
    option = int(input(" >>  "))

    num_processes = 15
    if option == 1:
        # Get all profiles and split and scrape them in 10 different processes

        df = get_db_connection().readDBtable(tablename='profiles', selectOptions=ProfilesSpider.primary_key)
        stored_profiles = df[ProfilesSpider.primary_key].tolist()
        ProfilesPipeline.my_logger.debug('Obtained list of stored profiles')
        # Init crawlers for each process
        jobs = list()

        queue = multiprocessing.Queue()
        for i in range(1, num_processes + 1):
            p = multiprocessing.Process(target=selenium4profiles.get_urls_scrapy,
                                        args=(i, stored_profiles, num_processes, queue))
            jobs.append(p)
            p.start()
        finished_processes = 0
        scrapy_spiders = 0
        spider_parameters = list()
        spider_number = list()
        while finished_processes < num_processes:
            bids_to_scrapy = queue.get()
            if bids_to_scrapy is not None:
                scrapy_spiders += 1
                spider_parameters.append(bids_to_scrapy)
                spider_number.append(scrapy_spiders)
                if len(spider_parameters) == num_processes:
                    p = multiprocessing.Process(target=run_profiles_spiders, args=(spider_parameters, spider_number))
                    p.start()
                    p.join()
                    spider_parameters = list()
                    spider_number = list()
            else:
                finished_processes += 1

        for proc in jobs:
            proc.join()

    elif option == 2:
        # Get all profiles and split and scrape them in 10 different processes

        df = get_db_connection().readDBtable(tablename='bids', selectOptions=BidsSpider.primary_key)
        stored_bids = df[BidsSpider.primary_key].tolist()
        BidsPipeline.my_logger.debug('Obtained list of stored bids')
        # Init crawlers for each process
        jobs = list()

        queue = multiprocessing.Queue()
        for i in range(1, num_processes + 1):
            p = multiprocessing.Process(target=selenium4bids.get_urls_scrapy,
                                        args=(i, stored_bids, num_processes, queue))
            jobs.append(p)
            p.start()
        finished_processes = 0
        scrapy_spiders = 0
        spider_parameters = list()
        spider_number = list()
        while finished_processes < num_processes:
            bids_to_scrapy = queue.get()
            if bids_to_scrapy is not None:
                scrapy_spiders += 1
                spider_parameters.append(bids_to_scrapy)
                spider_number.append(scrapy_spiders)
                if len(spider_parameters) == num_processes:
                    p = multiprocessing.Process(target=run_bids_spiders, args=(spider_parameters, spider_number))
                    p.start()
                    p.join()
                    spider_parameters = list()
                    spider_number = list()
            else:
                finished_processes += 1

        for proc in jobs:
            proc.join()

    elif option == 3:
        bids = get_db_connection().readDBtable(tablename='bids', selectOptions='Expediente_Licitacion, '
                                                                             'Enlace_A_Pliego, '
                                                                             'Formato_Pliego',
                                             filterOptions="Enlace_A_Pliego IS NOT NULL and Formato_Pliego != "
                                                           "'unparsed'")
        docs = get_db_connection().readDBtable(tablename='docs', selectOptions='Expediente_Licitacion')
        links = bids['Enlace_A_Pliego'].tolist()
        bids = bids['Expediente_Licitacion'].tolist()
        formats = bids['Formato_Pliego'].tolist()
        stored_docs = docs['Expediente_Licitacion'].tolist()
        doc_logger.debug('Obtained list of stored bids')
        array_size = len(bids) // num_processes
        split_bids = list(split_array(bids, array_size))
        split_links = list(split_array(links, array_size))
        split_formats = list(split_array(formats, array_size))

        if len(split_bids) > num_processes:
            split_bids[:2] += split_bids[-1]
            split_links[:2] += split_links[-1]
            split_formats[:2] += split_formats[-1]

            del split_bids[-1]
            del split_links[-1]
            del split_formats[-1]

        jobs = list()

        for i in range(1, num_processes + 1):
            doc_urls = dict()
            update = update
            stored_docs = stored_docs

            #  Generate dict in proper format for later processing
            for j, value in enumerate(split_links[i-1]):
                if split_bids[i-1][j] not in stored_docs:
                    bid_specs = {'bid_name': split_bids[i-1][j], 'doc_url': value}
                    if split_formats[i-1][j] in doc_urls.keys():
                        doc_urls[split_formats[i-1][j]].append(bid_specs)
                    else:
                        doc_urls[split_formats[i-1][j]] = [bid_specs]
            p = multiprocessing.Process(target=get_pdf_text, args=(doc_urls['pdf'], i))
            jobs.append(p)
            p.start()

        for proc in jobs:
            proc.join()


def run_bids_spiders(bids2scrapy, spider_numbers):
    process = CrawlerProcess(
        ({'USER_AGENT': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}))
    for index, bids in enumerate(bids2scrapy):
        urls = bids[0]
        bid_names = bids[1]
        spider_number = spider_numbers[index]
        process.crawl(BidsSpider, bid_names, urls, spider_number)
    process.start()


def run_profiles_spiders(bids2scrapy, spider_numbers):
    process = CrawlerProcess(
        ({'USER_AGENT': 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'}))
    for index, bids in enumerate(bids2scrapy):
        urls = bids[0]
        bid_names = bids[1]
        spider_number = spider_numbers[index]
        process.crawl(ProfilesSpider, bid_names, urls, spider_number)
    process.start()


if __name__ == '__main__':
    start_main_menu()
