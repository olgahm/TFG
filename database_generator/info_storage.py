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
            table_df = get_db_connection().readDBtable(tablename=table, selectOptions='*')
            table_dict = dict()
            fields = list(table_df)
            for field in fields:
                table_dict[field] = table_df[field].tolist()
            db_info[table] = table_dict
        except:
            continue
    return db_info


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
                # If the item is considered new, pass to update list
                to_update.append(item)
        if item.get('storage_mode', ''):
            del item['storage_mode']
    if final_to_insert:
        to_insert = final_to_insert

    # pk = get_primary_key(db_table)
    # df = DataFrame(item)
    # print(df)
    # for field in item:
    #     processed_item[field] = [item[field]]
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
            else:
                db_logger.debug(f'Duplicate entry in table {db_table}. Reloading stored data in memory...')
                stored_data = update_data() # TODO: try not using this every time. Index primary keys and ask for the
                # remaining info if needed
            # dup = re.search("Duplicate entry '(.*)' for key 'PRIMARY'", str(e)).group(1)
            # Get duplicated element
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
            data = item_to_database(items, db_table)
            if data:
                return data
            else:
                return stored_data
        elif 'Data too long' in str(e):
            if db_table == 'texts':
                # Error indicating that the text we are trying to store is way bigger than mysql maximum allowed size.
                # Split item into 2 and try again
                text = items[0]['pliego_tecnico'].split()
                text_1, text_2 = ' '.join(text[:len(text) // 2]), ' '.join(text[len(text) // 2:])
                item_1 = items[0].copy()
                item_2 = items[0].copy()
                item_1['bid_id'] += '_1'
                item_2['bid_id'] += '_2'
                item_1['pliego_tecnico'] = text_1
                item_2['pliego_tecnico'] = text_2
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
    if 'bid_id' in item:
        # Since all tables have a bid_id, check if current item has a stored bid_id
        if any(item['bid_id'] == stored_bid for stored_bid in stored_data['bid_id']):
            rows = stored_data['bid_id'].count(item['bid_id'])
            if rows > 1:
                indexes = [index for index, value in enumerate(stored_data['bid_id']) if
                           item['bid_id'] == stored_data['bid_id'][index]]
            else:
                indexes = [stored_data['bid_id'].index(item['bid_id'])]
            this_item = [str(item[key]) for key in item if item[key] is not None]
            for index in indexes:
                stored_item = list()
                for field in stored_data:
                    if stored_data[field][index] is not None and str(stored_data[field][index]) != 'nan':
                        stored_item.append(str(stored_data[field][index]))
                # Check if there are more fields in the stored entry than current one
                if len(stored_item) >= len(this_item):
                    if len(stored_item) > len(this_item):
                        duplicated = True
                    elif sorted(stored_item) == sorted(this_item):
                        duplicated = True
                else:
                    duplicated = False
        else:
            duplicated = False
    else:
        # if ')' in item['nombre']:
        #     print('Nombre raro')
        if any(item['nombre'].lower().strip() == stored_org.lower().strip() for stored_org in stored_data['nombre']):
            item['storage_mode'] = 'update'
            stored_org_names = [stored_org.lower().strip() for stored_org in stored_data['nombre']]
            index = stored_org_names.index(item['nombre'].lower().strip())
            this_item = [str(item[key]).lower().strip() for key in item if
                         item[key] is not None and key != 'storage_mode']
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
