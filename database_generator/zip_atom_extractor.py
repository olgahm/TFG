import requests
from io import BytesIO
import zipfile
from database_generator.atom_parser import process_xml_atom
from database_generator.info_storage import item_to_database
from database_generator.atom_parser import clean_elements
from database_generator.info_storage import update_data
import xml.etree.ElementTree


def zip_processing(zip_urls, manager):
    # update_da(db_data)
    for url in zip_urls:
        response = requests.get(url)
        with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
            for zipinfo in zip_file.infolist():
                with zip_file.open(zipinfo) as atom_file:
                    bids_xml = xml.etree.ElementTree.parse(atom_file)
                    root = clean_elements(bids_xml.getroot())
                    items = process_xml_atom(root, manager)
                    for item in items:
                        db_table = item.pop('table_name')
                        item_to_database(item, db_table)
                    manager[1] = update_data()
        with open('extracted_from_zips.txt', 'a') as f:
            f.writelines(url)
