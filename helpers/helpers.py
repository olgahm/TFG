#-*- coding: utf-8 -*-
"""
helpers.py
Functions to ease some needed tasks
"""

import re

from unidecode import unidecode

from helpers.BaseDMsql import BaseDMsql
from setup.config import DB_CONNECTION_PARAMS, DB_TABLE_STRUCTURE


def get_db_connection():
    """Init database connection

    :return: Database connection object
    """
    return BaseDMsql(db_name=DB_CONNECTION_PARAMS['db_name'], db_connector=DB_CONNECTION_PARAMS['db_connector'],
                     db_server=DB_CONNECTION_PARAMS['db_server'], db_user=DB_CONNECTION_PARAMS['db_user'],
                     db_password=DB_CONNECTION_PARAMS['db_password'])


def split_array(array, size):
    """Function to split array in N arrays of length 'size'

    :param array: Array to split
    :param size: Length of arrays
    :return: List of array segments
    """
    arrays = []
    while len(array) > size:
        pice = array[:size]
        arrays.append(pice)
        array = array[size:]
    arrays.append(array)
    return arrays


def insert_or_update_records(db_conn, items, db_table, ref_field=None):
    """Function to insert or update records in database

    :param db_conn: BaseDMsql object. DB connection
    :param items: List of dicts with records to insert
    :param db_table: Target table
    :param ref_field: Reference field for updating records
    :return:
    """

    if ref_field is not None:
        pk = ref_field
    else:
        pk = DB_TABLE_STRUCTURE[db_table]['primary_key']
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
            print('Data too long')
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
                insert_or_update_records([item_1], 'texts', )
                insert_or_update_records([item_2], 'texts', )
            else:
                for item in to_insert:
                    item['nombre'] = re.sub('\d{2}\)', '', item['nombre'])
                    if len(item['nombre']) > 250:
                        item['nombre'] = item['nombre'][:250]
                insert_or_update_records(to_insert, db_table, )
        elif 'Incorrect string value' in str(e):
            items[0]['pliego_tecnico'] = unidecode(items[0]['pliego_tecnico'])
            insert_or_update_records(items, db_table, )
        else:
            print(str(e))


def remove_duplicates(table=None):
    """Function to remove duplicate rows from tables.
    This is done by taking all distinct records and writing them to a new table. This new
    table becomes the main table and the old one is dropped

    :return:
    """
    mysql_connection = get_db_connection()
    if table is not None:
        tablenames = [table]
    else:
        tablenames = mysql_connection.getTableNames()
    for tablename in tablenames:
        print(tablename)
        if DB_TABLE_STRUCTURE[tablename]['primary_key'] is None:
            # Rename table for taking only unique records
            column_names = ', '.join(mysql_connection.getColumnNames(tablename))
            commands = [f'RENAME TABLE {tablename} to {tablename}_tmp',
                        f'CREATE TABLE {tablename} SELECT * FROM {tablename}_tmp GROUP BY {column_names}',
                        f'ALTER TABLE {tablename} CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci',
                        f'DROP TABLE {tablename}_tmp']
            if tablename != 'texts':
                commands.append(f'CREATE INDEX bid_id_index ON {tablename} (bid_id)')
            if tablename == 'docs' or tablename == 'texts':
                commands.append(f'CREATE INDEX doc_url_index ON {tablename} (doc_url)')
            mysql_connection.execute_commands(commands)


def construct_dupes_dict(in_list):
    """Function to transform list with duplicate values into dict such that:
            - Keys are distinct values of input list
            - Key values are lists of indexes where value occurs in input list

    :param list:
    :return:
    """
    out_dict = dict()
    for index, key in enumerate(in_list):
        if key not in out_dict:
            out_dict[key] = list()
        out_dict[key].append(index)
    return out_dict