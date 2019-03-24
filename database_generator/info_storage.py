# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

from datetime import datetime
from datetime import timedelta
from logging import FileHandler
from logging import Formatter
import logging
import re
import os
import json

from pandas import DataFrame

from config import get_db_connection

# Init log file

MY_LOG_FORMAT = ("%(asctime)s %(levelname)s %(message)s")
my_logger = logging.getLogger(__name__)
my_logger.setLevel(logging.DEBUG)
my_logger_file_handler = FileHandler('database_generator/log/docs_parser.log')
my_logger_file_handler.setLevel(logging.DEBUG)
my_logger_file_handler.setFormatter(Formatter(MY_LOG_FORMAT))
my_logger.addHandler(my_logger_file_handler)

DB_GEN_PATH = os.path.dirname(os.path.abspath(__file__))


def get_all_table_names():
    with open(os.path.join(DB_GEN_PATH, 'DB_tables.json')) as f:
        table_info = json.loads(f.read())
    return [table for table in table_info]


def get_all_tables_info():
    tables = get_all_table_names()
    db_info = dict()
    for table in tables:
        try:
            table_df = get_db_connection().readDBtable(tablename=table, selectOptions='*')
            table_dict = dict()
            fields = list(table_df)
            for field in fields:
                table_dict[field] = table_df[field].tolist()
            db_info[table] = table_dict
        except:
            continue
    return db_info


def item_to_database(item, db_table):
    return item2dataframe(item, db_table)


def item2dataframe(item, db_table):
    """Function to process DocItem in the database.
    Since we are processing strings of unknown length and some of them may be too long to fit in one MySQL field,
    if needed the text is split in halves until it fits in the row. This way we may have more than one entry for the
    same doc.

    :param item: Item to be stored in the database
    :return: Last stored item
    """
    processed_item = dict()
    storage_mode = item.get('storage_mode', '')
    if storage_mode:
        del item['storage_mode']
    pk = get_primary_key(db_table)
    for field in item:
        if item[field] is not None:
            processed_item[field] = [item[field]]
    try:
        # Get primary key
        if pk and storage_mode == 'new':
            get_db_connection().upsert(db_table, pk, DataFrame(processed_item))
        elif pk and storage_mode == 'update':
            fields = [field for field in processed_item]
            values = [[item[pk]] + [item[field] for field in fields]]
            get_db_connection().setField(db_table, pk, fields, values)
        else:
            columns = [field for field in item]  # Name of columns
            values = [[item[key] for key in item]]  # Values
            get_db_connection().insertInTable(db_table, columns, values)
        return processed_item
    except BaseException as e:
        print(e)


def get_primary_key(table):
    with open(os.path.join(DB_GEN_PATH, 'DB_tables.json')) as f:
        table_info = json.loads(f.read())
    return table_info[table]['primary_key']


def get_mandatory_keys(table):
    with open(os.path.join(DB_GEN_PATH, 'DB_tables.json')) as f:
        table_info = json.loads(f.read())
    return table_info[table]['fields']


def update_data(data=None):
    # Meter las cosas guardadas a mano sin consultar en la base de datos para reducir utilizacion del disco
    if data is not None:
        db_stored_info = data
    else:
        db_stored_info = get_all_tables_info()
    return db_stored_info


def get_data_from_table(db_stored_info, table):
    return db_stored_info[table]


def is_deleted(bid_uri, bid_metadata, items_in_database):
    """Function to check if bid has a deletion timestamp

    :param bid_uri:
    :return:
    """
    stored_bids = items_in_database['bids']['bid_uri']  # List of stored bids
    stored_offsets = items_in_database['bids']['deleted_at_offset']  # List of deletion_times
    deleted = False
    bid_metadata['storage_mode'] = 'new'
    if any(bid_uri == stored_bid for stored_bid in stored_bids):
        index = stored_bids.index(bid_uri)
        deletion_date = stored_offsets[index]
        if deletion_date:
            deleted = True
        else:
            deleted = False
            bid_metadata['storage_mode'] = 'update'
    return deleted


def is_new_or_update(bid_uri, last_update, offset, bid_metadata, items_in_database):
    """Function to check there is information for input bid. If so, check if the bid being processed contains newer
    information than stored one.

    :param bid_name: bid to check
    :param last_update: last update date
    :param offset: last update UTC offset

    :return:
    """
    stored_bids = items_in_database['bids']['bid_uri']  # List of stored bids
    stored_last_updates = items_in_database['bids']['last_updated']  # List of update times
    stored_last_update_offsets = items_in_database['bids']['last_updated_offset']  # List of update offsets
    bid_metadata['storage_mode'] = 'new'
    update = False
    # Check if bid is stored in database
    if any(bid_uri == stored_bid for stored_bid in stored_bids):
        # Check last update time for stored bid. If not, the bid has already been deleted
        index = stored_bids.index(bid_uri)
        stored_last_update = str(stored_last_updates[index])
        stored_offset = stored_last_update_offsets[index]
        # If the bid has been deleted and there is no info
        if stored_offset is None:
            update = True
            bid_metadata['storage_mode'] = 'update'
        else:
            if stored_offset != offset:
                hours, minutes = offset.split(':')
                stored_hours, stored_minutes = stored_offset.split(':')
                hours = int(hours)
                stored_hours = int(stored_hours)
                minutes = int(minutes)
                stored_minutes = int(stored_minutes)
                last_update = datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S") - timedelta(hours=hours,
                                                                                              minutes=minutes)
                stored_last_update = datetime.strptime(stored_last_update, "%Y-%m-%d %H:%M:%S") - timedelta(
                    hours=stored_hours, minutes=stored_minutes)
            else:
                last_update = datetime.strptime(last_update, "%Y-%m-%d %H:%M:%S")
                stored_last_update = datetime.strptime(stored_last_update, "%Y-%m-%d %H:%M:%S")
            # If current last update time is more recent
            if last_update > stored_last_update:
                update = True
                bid_metadata['storage_mode'] = 'update'
    else:
        # If bid is new
        update = True
        bid_metadata['storage_mode'] = 'new'
    return update


def is_stored(table, item, storage_mode, items_in_database, answers_set=None):
    """Function to determine if there is an item in the database exactly equal to the one it is going to be stored

    :param table: Table to query
    :param item: Item to be checked
    :return:
    """
    stored_data = items_in_database[table]
    duplicated = False
    # Check if current item is related to a bid
    if item.get('bid_id', ''):
        # Since all tables have a bid_id, check if current item has a stored bid_id
        if any(item['bid_id'] == stored_bid for stored_bid in stored_data['bid_id']):
            if storage_mode == 'new':  # The item may be stored but not the item
                rows = stored_data['bid_id'].count(item['bid_id'])
                if rows > 1:
                    indexes = [index for index, value in enumerate(stored_data['bid_id']) if
                               item['bid_id'] == stored_data['bid_id'][index]]
                else:
                    indexes = [stored_data['bid_id'].index(item['bid_id'])]
                this_item = [str(item[key]) for key in item if key != 'table_name']
                for index in indexes:
                    stored_item = list()
                    for field in stored_data:
                        if stored_data[field][index] is not None and str(stored_data[field][index]) != 'nan':
                            stored_item.append(str(stored_data[field][index]))
                    # Check if there are more fields in the stored entry than current one
                    if len(this_item) == len(stored_item):
                        if sorted(stored_item) == sorted(this_item):
                            duplicated = True
                            break
            else:
                get_db_connection().deleteRowTables(table, f"bid_id='{item['bid_id']}'")
                update_data()
                duplicated = False
        else:
            duplicated = False
    else:
        # print('update variable for stored data')
        # update_data()
        # If there is no bid id, this is an organization

        if any(item['nombre'] == stored_org for stored_org in stored_data['nombre']):
            if storage_mode == 'new':
                index = stored_data['nombre'].index(item['nombre'])
                this_item = [str(item[key]) for key in item if key != 'table_name']
                stored_item = list()
                for field in stored_data:
                    if stored_data[field][index] is not None:
                        stored_item.append(str(stored_data[field][index]))
                if len(stored_item) == len(this_item):
                    if sorted(stored_item) == sorted(this_item):
                        duplicated = True
            else:
                item['storage_mode'] = 'update'
                duplicated = False
        else: # If not stored in database, it may appear in the answer set
            item['storage_mode'] = 'new'
            storing_orgs = [item for item in answers_set if item['table_name'] == 'orgs']
            for storing_org in storing_orgs:
                if storing_org['nombre'] == item['nombre']:
                    this_org = [item[field] for field in item]
                    to_store_org = [storing_org[field] for field in storing_org]
                    if len(this_org) > len(to_store_org):
                        duplicated = False
                        del storing_org
                    else:
                        duplicated = True
                    break

    return duplicated
