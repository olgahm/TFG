# -*- coding: utf-8 -*-

from datetime import datetime
from datetime import timedelta
import os
import json
from config import db_logger
import traceback
from config import get_db_connection
import re
from unidecode import unidecode

import itertools
import sys
from config import split_array
from multiprocessing import Pool

DB_GEN_PATH = os.path.dirname(os.path.abspath(__file__))
stored_pk = list()


def get_all_table_names():
    """Function to process the file definig the database structure and get name of all tables

    :return: list of table names
    """
    with open(os.path.join(DB_GEN_PATH, 'DB_tables.json')) as f:
        table_info = json.loads(f.read())
    return [table for table in table_info]


def get_all_tables_info():
    """Function to load data in database in memory

    :return: dict with database information
    """
    tables = get_all_table_names()
    db_info = dict()
    for table in tables:
        try:
            table_dict = get_data_from_table(table)
            db_info[table] = table_dict
        except:
            continue
    return db_info





def item_to_database(db_conn, items, db_table, recent_data=None):
    """Function to process DocItem in the database.
    Since we are processing strings of unknown length and some of them may be too long to fit in one MySQL field,
    if needed the text is split in halves until it fits in the row. This way we may have more than one entry for the
    same doc.

    :param item: Item to be stored in the database
    :return: Last stored item
    """
    # global stored_pk
    pk = get_primary_key(db_table)
    to_update = [item for item in items if item.get('storage_mode', '') == 'update']
    to_insert = [item for item in items if item.get('storage_mode', '') != 'update']
    for item in to_update:
        if item.get('storage_mode'):
            del item['storage_mode']
    for item in to_insert:
        if item.get('storage_mode'):
            del item['storage_mode']
    try:
        if to_insert:
            rows = list()
            columns = [field for field in to_insert[0]]
            for item in to_insert:
                rows.append([item[field] for field in columns])
            db_conn.insertInTable(db_table, columns, rows)
        if to_update:
            for item in to_update:
                fields = [field for field in item if item[field] is not None]
                values = [[item[pk]] + [item[field] for field in fields]]
                db_conn.setField(db_table, pk, fields, values)
    except BaseException as e:
        if 'Duplicate' in str(e):
            print('Duplicate')
            # if recent_data:
            #     stored_data = recent_data
            # elif db_table != 'texts':
            #     db_logger.debug(f'Duplicate entry in table {db_table}. Reloading stored data in memory...')
            #     stored_data = {'bids': get_db_bid_info(), 'orgs': get_data_from_table('orgs')}
            # elif db_table == 'bids':
            #     items = list()
            #     for item in to_insert:
            #         if 'deleted_at' in item:
            #             if not deleted_bid(item[pk], item, stored_data):
            #                 items.append(item)
            #         else:
            #             if is_new_or_update(item[pk], item['last_updated'], item['last_updated_offset'], item,
            #                                 stored_data):
            #                 items.append(item)
            # elif db_table == 'texts':
            #     items = list()
            #     for item in to_insert:
            #         item['bid_id'] += '_1'
            #         items.append(item)
            # data = item_to_database(items, db_table)
            # if data:
            #     return data
            # else:
            #     return stored_data
        elif 'Data too long' in str(e):
            if db_table == 'texts':
                # Error indicating that the text we are trying to store is way bigger than mysql maximum allowed size.
                # Split item into 2 and try again recursively until text fits
                text = items[0]['texto_original'].split()
                text_1, text_2 = ' '.join(text[:len(text) // 2]), ' '.join(text[len(text) // 2:])
                item_1 = items[0].copy()
                item_2 = items[0].copy()
                item_1['bid_id'] += '_1'
                item_2['bid_id'] += '_2'
                item_1['texto_original'] = text_1
                item_2['texto_original'] = text_2
                item_to_database([item_1], 'texts')
                item_to_database([item_2], 'texts')
            else:
                for item in to_insert:
                    item['nombre'] = re.sub('\d{2}\)', '', item['nombre'])
                    if len(item['nombre']) > 250:
                        item['nombre'] = item['nombre'][:250]
                item_to_database(to_insert, db_table)
        elif 'Incorrect string value' in str(e):
            items[0]['pliego_tecnico'] = unidecode(items[0]['pliego_tecnico'])
            item_to_database(items, db_table)
        else:
            print(str(e))


def get_primary_key(table):
    with open(os.path.join(DB_GEN_PATH, 'DB_tables.json')) as f:
        table_info = json.loads(f.read())
    return table_info[table]['primary_key']


def get_mandatory_keys(table):
    with open(os.path.join(DB_GEN_PATH, 'DB_tables.json')) as f:
        table_info = json.loads(f.read())
    return table_info[table]['fields']


def deleted_bid(bid_uri, bid_metadata, bid_info_db):
    """

    :param bid_uri: Bid id
    :param bid_metadata: Bid data
    :param bid_info_db: dict with items stored in database
    :return:
    """
    stored_bids = bid_info_db['bid_uri']  # List of stored bids
    stored_offsets = bid_info_db['deleted_at_offset']  # List of deletion_times
    deleted = False
    bid_metadata['storage_mode'] = 'new'
    if bid_uri in stored_bids:
        db_logger.debug(f'Bid {bid_uri} already stored')
        index = stored_bids.index(bid_uri)
        deletion_date = stored_offsets[index]
        if deletion_date is None:
            db_logger.debug(f'Storing deletion date for bid {bid_uri}')
            deleted = False
            bid_metadata['storage_mode'] = 'update'
        else:
            db_logger.debug(f'Bid {bid_uri} already deleted from PCSP')
            deleted = True
    else:
        db_logger.debug(f'Storing deleted bid {bid_uri}')
    return deleted


def new_bid(bid_uri, bid_metadata, bid_info_db):
    stored_bids = bid_info_db['bid_uri']  # List of stored bids
    if bid_uri in stored_bids:
        bid_metadata['storage_mode'] = 'update'
        return False
    else:
        bid_metadata['storage_mode'] = 'new'
        return True


def more_recent_bid(bid_uri, last_updated, offset, stored_last_update, stored_offset):
    if stored_offset is None:  # This means that the bid appears as deleted but there is not actual data for the bid
        return True
    else:
        if stored_offset != offset:  # If different offsets, transform times to UTC
            hours, minutes = offset.split(':')
            stored_hours, stored_minutes = stored_offset.split(':')
            hours = int(hours)
            stored_hours = int(stored_hours)
            minutes = int(minutes)
            stored_minutes = int(stored_minutes)
            last_update = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S") - timedelta(hours=hours, minutes=minutes)
            stored_last_update = datetime.strptime(str(stored_last_update), "%Y-%m-%d %H:%M:%S") - timedelta(
                hours=stored_hours, minutes=stored_minutes)
        else:
            last_update = datetime.strptime(last_updated, "%Y-%m-%d %H:%M:%S")
            stored_last_update = datetime.strptime(str(stored_last_update), "%Y-%m-%d %H:%M:%S")
        if last_update > stored_last_update:
            db_logger.debug(f'Bid {bid_uri} is more recent than stored entry. Updating bid...')
            return True
        else:
            return False


def remove_duplicates():
    """Function to remove duplicate rows from tables.
    This is done by taking all distinct records and writing them to a new table. This new
    table becomes the main table and the old one is dropped

    :return:
    """
    mysql_connection = get_db_connection()
    tablenames = mysql_connection.getTableNames()
    tablenames.remove('bids')
    for tablename in tablenames:
        db_logger.debug(f'Removing duplicates for table {tablename}')
        # Rename table for taking only unique records
        column_names = ', '.join(mysql_connection.getColumnNames(tablename))
        commands = [f'RENAME TABLE {tablename} to {tablename}_tmp',
                    f'CREATE TABLE {tablename} SELECT * FROM {tablename}_tmp GROUP BY {column_names}',
                    f'DROP TABLE {tablename}_tmp']
        mysql_connection.execute_commands(commands)
    db_logger.debug('All duplicates removed')


