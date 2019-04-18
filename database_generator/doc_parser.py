from io import BytesIO
import re
import requests
from tika import parser
import zipfile
import rarfile

from database_generator.info_storage import item_to_database
from config import get_db_connection
from config import text_logger
from bs4 import BeautifulSoup


def store_document_text(url, bid_id, doc_id, hash):
    doc_format = doc_id.split('.')[-1].lower()
    text_logger.debug(f'Processing {doc_format} document')
    doc_formats = ['pdf', 'doc', 'docx', 'zip', 'rar', 'rtf', 'html', 'htm']
    if doc_format in doc_formats:
        response = requests.get(url)
        to_parse = response.content
        content_type = response.headers['Content-Type']
        if doc_format not in content_type:
            # See url content
            soup = BeautifulSoup(to_parse, 'html.parser')
            doc_link = soup.find('meta')
            if doc_link is not None:
                relative_link = doc_link['content']
                relative_link = re.search("\\'(.*)\\'", relative_link).group(1)
                abs_link = f'https://contrataciondelestado.es{relative_link}'
                response = requests.get(abs_link)
                to_parse = response.content
            else:
                if 'Documento no accesible' in response.content.decode('utf8'):
                    get_db_connection().deleteRowTables('docs', f"doc_url='{url}' and doc_type='tecnico'")
                    text_logger.debug(f'Document in {url} not available. Deleting entry in database...')
                elif 'htm' in doc_format:
                    text_logger.debug(f'Study this {doc_format} url: {url}')
                else:
                    text_logger.debug(f'Unprocessed url {url}')
                return
        if 'zip' in doc_format:
            file_counter = 0
            with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                for zipinfo in zip_file.infolist():
                    if zipinfo.filename[-1] != '/':
                        zipfile_doc_format = zipinfo.filename.split('.')[-1]
                        if zipfile_doc_format.lower() in doc_formats:
                            with zip_file.open(zipinfo) as file:
                                to_parse = file.read()
                                text, ocr = get_pdf_text(to_parse)
                                if text:
                                    file_counter += 1
                                    item = {'bid_id': f'{bid_id}_{file_counter}', 'pliego_tecnico': text,
                                            'storage_mode': 'new', 'ocr': ocr}
                                    item_to_database([item], 'texts')
                                else:
                                    print(f'We may need and OCR {url}')
                        else:
                            print(f'Unstudied format inside zip {zipfile_doc_format}')
            return
        elif 'rar' in doc_format:
            file_counter = 0
            with rarfile.RarFile(BytesIO(response.content)) as rar_file:
                for rarinfo in rar_file.infolist():
                    if rarinfo.filename[-1] != '/':
                        rarfile_doc_format = rarinfo.filename.split('.')[-1]
                        if rarfile_doc_format.lower() in doc_formats:
                            with rar_file.open(rarinfo) as file:
                                to_parse = file.read()
                                text, ocr = get_pdf_text(to_parse)
                                if text:
                                    file_counter += 1
                                    item = {'bid_id': f'{bid_id}_{file_counter}', 'pliego_tecnico': text,
                                            'storage_mode': 'new', 'ocr': ocr}
                                    item_to_database([item], 'texts')
                                else:
                                    print(f'We may need and OCR {url}')
                        else:
                            print(f'Unstudied format inside rar {rarfile_doc_format}')
            return
    else:
        print(f'{doc_format}: {url}')
        return
    text, ocr = get_pdf_text(to_parse)
    if text:
        item = {'bid_id': bid_id, 'pliego_tecnico': text, 'storage_mode': 'new', 'ocr': ocr}
        item_to_database([item], 'texts')
    else:
        print(f'We may need and OCR {url}')


def get_pdf_text(buffer):
    while True:
        try:
            parsed_data = parser.from_buffer(buffer)
            break
        except:
            continue
    ocr = False
    if 'content' in parsed_data and parsed_data['content'] is None:
        headers = {'X-Tika-PDFextractInlineImages': 'true'}
        parsed_data = parser.from_buffer(buffer, headers=headers)
        ocr = True
        if parsed_data['content'] is None:
            return '', False
    text = parsed_data['content']
    final_text = re.sub("[	 \n\s]+", ' ', text).strip()
    final_text = final_text.replace('\\\\', '\\')
    return final_text, ocr
