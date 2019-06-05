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


def get_data_from_table(table):
    table_df = get_db_connection().readDBtable(tablename=table, selectOptions='*')
    table_dict = dict()
    fields = list(table_df)
    for field in fields:
        table_dict[field] = table_df[field].tolist()
    return table_dict


def item_to_database(items, db_table, recent_data=None):
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
    queued = list()
    final_to_insert = list()
    for item in to_insert:
        if pk is not None:
            if item[pk].lower().strip() not in queued:
                final_to_insert.append(item)
                queued.append(item[pk].lower().strip())
            elif db_table == 'bids':
                # If the item is considered new but in queue, pass to update list
                to_update.append(item)
        if item.get('storage_mode', ''):
            del item['storage_mode']
    if final_to_insert:
        to_insert = final_to_insert

    # Check if bid queued to be updated are more recent
    final_to_update = list()
    if to_update and db_table == 'bids':
        unique_bid_ids = list(dict.fromkeys([item[pk] for item in to_update]))
        for bid in unique_bid_ids:
            null_date_indexes = [index for index, item in enumerate(to_update) if
                                 item[pk] == bid and to_update[index]['last_updated'] is None]
            for index in null_date_indexes:
                final_to_update.append(to_update[index])
            date_indexes = [index for index, item in enumerate(to_update) if
                            item[pk] == bid and to_update[index]['last_updated'] is not None]
            dates = [datetime.strptime(to_update[index]['last_updated'], '%Y-%m-%d %H:%M:%S') for index in date_indexes]
            if dates:
                if len(dates) > 1:
                    maximum_date = max(date for date in dates)
                    max_date_index = date_indexes[dates.index(maximum_date)]
                else:
                    max_date_index = date_indexes[0]
                # Check if there is a new bid with with id and compare times
                if any(item[pk] == bid for item in to_insert):
                    insert_bid_index = [index for index, item in enumerate(to_insert) if item[pk] == bid][0]
                    insert_last_update = to_insert[insert_bid_index]['last_updated']
                    if insert_last_update is not None:
                        if datetime.strptime(to_update[max_date_index]['last_updated'],
                                             '%Y-%m-%d %H:%M:%S') > datetime.strptime(insert_last_update,
                                                                                      '%Y-%m-%d %H:%M:%S'):
                            print(to_update[max_date_index]['last_updated'])
                            print(insert_last_update)
                            sys.exit(0)
                            to_insert[insert_bid_index] = to_update[max_date_index]
                        continue
                final_to_update.append(to_update[max_date_index])
    if final_to_update:
        to_update = final_to_update
    try:
        if to_insert:
            rows = list()
            columns = [field for field in to_insert[0]]
            for item in to_insert:
                rows.append([item[field] for field in columns])
            get_db_connection().insertInTable(db_table, columns, rows)
        if to_update:
            for item in to_update:  # TODO: Try to group items with same fields to change in order to save time
                fields = [field for field in item if item[field] is not None]
                values = [[item[pk]] + [item[field] for field in fields]]
                get_db_connection().setField(db_table, pk, fields, values)
    except BaseException as e:
        if 'Duplicate' in str(e):
            if recent_data:
                stored_data = recent_data
            elif db_table != 'texts':
                db_logger.debug(f'Duplicate entry in table {db_table}. Reloading stored data in memory...')
                stored_data = {'bids': get_db_bid_info(), 'orgs': get_data_from_table('orgs')}
            if db_table == 'orgs':
                items = list()
                for item in to_insert:
                    if not is_stored('orgs', item, 'new', stored_data):
                        items.append(item)
            elif db_table == 'bids':
                items = list()
                for item in to_insert:
                    if 'deleted_at' in item:
                        if not is_deleted(item[pk], item, stored_data):
                            items.append(item)
                    else:
                        if is_new_or_update(item[pk], item['last_updated'], item['last_updated_offset'], item,
                                            stored_data):
                            items.append(item)
            elif db_table == 'texts':
                items = list()
                for item in to_insert:
                    item['bid_id'] += '_1'
                    items.append(item)
            data = item_to_database(items, db_table)
            if data:
                return data
            else:
                return stored_data
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


def is_deleted(bid_uri, bid_metadata, items_in_database):
    """

    :param bid_uri: Bid id
    :param bid_metadata: item for metadata
    :param items_in_database: dict with items stored in database
    :return:
    """
    stored_bids = items_in_database['bids']['bid_uri']  # List of stored bids
    stored_offsets = items_in_database['bids']['deleted_at_offset']  # List of deletion_times
    deleted = False
    bid_metadata['storage_mode'] = 'new'
    if any(bid_uri == stored_bid for stored_bid in stored_bids):
        db_logger.debug(f'Bid {bid_uri} already stored')
        index = stored_bids.index(bid_uri)
        deletion_date = stored_offsets[index]
        if deletion_date is not None:
            db_logger.debug(f'Bid {bid_uri} already deleted from PCSP')
            deleted = True
        else:
            db_logger.debug(f'Storing deletion date for bid {bid_uri}')
            deleted = False
            bid_metadata['storage_mode'] = 'update'
    else:
        db_logger.debug(f'Storing deleted bid {bid_uri}')
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
        db_logger.debug(f'Bid {bid_uri} already stored')
        # Check last update time for stored bid. If not, the bid has already been deleted
        index = stored_bids.index(bid_uri)
        stored_last_update = str(stored_last_updates[index])
        stored_offset = stored_last_update_offsets[index]
        # If the bid has been deleted and there is no info
        if stored_offset is None:
            db_logger.debug(f'Bid {bid_uri} has only deletion information. Updating bid...')
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
                db_logger.debug(f'Bid {bid_uri} is more recent than stored entry. Updating bid...')
                update = True
                bid_metadata['storage_mode'] = 'update'
    else:
        # If bid is new
        db_logger.debug(f'Storing new bid {bid_uri}')
        update = True
        bid_metadata['storage_mode'] = 'new'
    return update


def is_stored(table, item, storage_mode, items_in_database):
    """Function to determine if there is an item in the database exactly equal to the one it is going to be stored

    :param table: Table to query
    :param item: Item to be checked
    :return:
    """
    stored_data = items_in_database[table]
    duplicated = False
    # Check if current item is related to a bid
    if any(item['nombre'].lower().strip() == stored_org.lower().strip() for stored_org in stored_data['nombre']):
        item['storage_mode'] = 'update'
        stored_org_names = [stored_org.lower().strip() for stored_org in stored_data['nombre']]
        index = stored_org_names.index(item['nombre'].lower().strip())
        this_item = [str(item[key]).lower().strip() for key in item if item[key] is not None and key != 'storage_mode']
        stored_item = list()
        for field in stored_data:
            if stored_data[field][index] is not None:
                stored_item.append(str(stored_data[field][index]).lower().strip())
        if len(stored_item) >= len(this_item):
            if len(stored_item) > len(this_item):
                db_logger.debug(f'Organization {item["nombre"]} already stored in database')
                duplicated = True
            elif sorted(stored_item) == sorted(this_item):
                db_logger.debug(f'Organization {item["nombre"]} already stored in database')
                duplicated = True
        else:
            duplicated = False
    else:
        item['storage_mode'] = 'new'
        duplicated = False
    return duplicated


def remove_duplicates():
    """Function to remove duplicate rows from tables in database without primary key

    :return:
    """
    table_names = get_all_table_names()
    primary_keys = [get_primary_key(table) for table in table_names]
    tables_w_dups = [table_names[index] for index, key in enumerate(primary_keys) if not key]
    for table in tables_w_dups:
        print(table)
        print(datetime.now())
        stored_data = get_data_from_table(table)
        unique_bid_ids = list(dict.fromkeys(stored_data['bid_id']))
        if len(unique_bid_ids) > 0:
            from functools import partial
            unique_bid_ids_split = split_array(unique_bid_ids, len(unique_bid_ids) // 4)
            func = partial(get_dupes, stored_data, table)
            with Pool() as pool:
                pool.map(func, unique_bid_ids_split)
            # for split in unique_bid_ids_split:
            #     get_dupes(stored_data, table, split)
        print(f'Finished checking dupes for table {table} at {datetime.now()}')


def get_dupes(stored_data, table, unique_bid_ids):
    stored_bid_ids = stored_data['bid_id']
    columns = [field for field in stored_data]
    to_insert = list()
    deletion_cond = str()
    for bid_id in unique_bid_ids:
        # print(bid_id)
        # print('In database')
        rows_4_id = [index for index, value in enumerate(stored_bid_ids) if bid_id == stored_bid_ids[index]]
        # print(len(rows_4_id))
        if len(rows_4_id) > 1:
            values_2_compare = list()
            for index in rows_4_id:
                values_2_compare.append([str(stored_data[field][index]) for field in stored_data])
            seen = set()
            distinct_rows = [x for x in values_2_compare if frozenset(x) not in seen and not seen.add(frozenset(x))]

            for row in distinct_rows:
                for index, item in enumerate(row):
                    if item.lower() == 'nan' or item == 'None':
                        row[index] = None
            # print(distinct_rows)
            # sys.exit(0)
            # All rows have the same field order, so go one by one checking if the item has a longer match
            copy_dis_rows = list()

            for this_row in distinct_rows:
                latest_entry = True
                for other_row in distinct_rows:
                    if set([value for value in this_row if value is not None]).issubset(
                            [value for value in other_row if value is not None]) and this_row != other_row:
                        print(this_row)
                        print(other_row)
                        print(distinct_rows)
                        latest_entry = False
                        break
                        sys.exit(0)
                if latest_entry:
                    copy_dis_rows.append(this_row)


            # print('Distinct')
            # print(len(distinct_rows))
            if len(values_2_compare) != len(copy_dis_rows):
                # print('Removing dupes and storing unique bid information')
                deletion_cond += f"bid_id='{bid_id}' OR "
                to_insert += copy_dis_rows
                if len(to_insert) > 500:
                    print('Deleting dupes...')
                    get_db_connection().deleteRowTables(table, deletion_cond[:-3])
                    deletion_cond = str()
                    print('Insert unique rows...\n\n\n\n\n')
                    try:
                        get_db_connection().insertInTable(table, columns, to_insert)
                    except:
                        print(columns)
                        print(to_insert)
                        sys.exit(0)
                    to_insert = list()
    if to_insert:
        print('Final insertion')
        print('Deleting dupes...')
        get_db_connection().deleteRowTables(table, deletion_cond[:-3])
        print('Inserting unique rows...')
        get_db_connection().insertInTable(table, columns, to_insert)


def get_db_bid_info():
    table_df = get_db_connection().readDBtable(tablename='bids', selectOptions='bid_uri, deleted_at_offset, '
                                                                               'last_updated_offset, last_updated')
    table_dict = dict()
    fields = list(table_df)
    for field in fields:
        table_dict[field] = table_df[field].tolist()
    return table_dict

