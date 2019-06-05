from io import BytesIO
import json
import logging
import re
import sys

from bs4 import BeautifulSoup
from textblob import TextBlob
from tika import parser
from unidecode import unidecode
import nltk
import rarfile
import requests
import zipfile

from mysql_helpers import BaseDMsql
from config import get_db_connection
from config import split_array
from setup_logger import text_extractor_logger as logger
from database_generator.db_helpers import item_to_database


def get_doc_format(url):
    response = requests.get(url)
    content = response.content
    content_type = response.headers['Content-Type']

def store_document_text(url, bid_id, doc_id, hash):
    doc_format = doc_id.split('.')[-1].lower()
    logger.debug(f'Processing {doc_format} document')
    doc_formats = ['pdf', 'doc', 'docx', 'zip', 'rar', 'rtf', 'html', 'htm']
    non_parsing_doc_formats = ['dwg', 'bc3']
    if doc_format in doc_formats:
        response = requests.get(url)
        to_parse = response.content
        content_type = response.headers['Content-Type']
        if doc_format not in content_type:
            # See url content.
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
                    logger.debug(f'Document in {url} not available. Deleting entry in database...')
                    get_db_connection().deleteRowTables('docs', f"doc_url='{url}' and doc_type='tecnico'")
                elif 'htm' in doc_format:
                    logger.debug(f'Study this {doc_format} url: {url}')
                else:
                    logger.debug(f'Unprocessed url {url}')
                return
        else:
            print(f'study this doc {url}')
        if 'zip' in doc_format:
            file_counter = 0
            with zipfile.ZipFile(BytesIO(response.content)) as zip_file:
                for zipinfo in zip_file.infolist():
                    if zipinfo.filename[-1] != '/':
                        zipfile_doc_format = zipinfo.filename.split('.')[-1]
                        if zipfile_doc_format.lower() in doc_formats and zipfile_doc_format.lower() not in \
                                non_parsing_doc_formats:
                            with zip_file.open(zipinfo) as file:
                                to_parse = file.read()
                                text, ocr = get_pdf_text(to_parse, url)
                                if text:
                                    file_counter += 1
                                    item = {'bid_id': f'{bid_id}_{file_counter}', 'texto_original': text,
                                            'storage_mode': 'new', 'ocr': ocr}
                                    item_to_database([item], 'texts')
                                else:
                                    print(f'We may need and OCR {url}')
                        elif zipfile_doc_format.lower() not in non_parsing_doc_formats:
                            print(f'Erase entries in db for url {url}')
                            print(f'Unstudied format inside zip {zipfile_doc_format}')
            return
        elif 'rar' in doc_format:
            file_counter = 0
            print('Found rar file')
            with rarfile.RarFile(BytesIO(response.content)) as rar_file:
                for rarinfo in rar_file.infolist():
                    if rarinfo.filename[-1] != '/':
                        rarfile_doc_format = rarinfo.filename.split('.')[-1]
                        if rarfile_doc_format.lower() in doc_formats and rarfile_doc_format.lower() not in \
                                non_parsing_doc_formats:
                            with rar_file.open(rarinfo) as file:
                                to_parse = file.read()
                                text, ocr = get_pdf_text(to_parse, url)
                                if text:
                                    file_counter += 1
                                    item = {'bid_id': f'{bid_id}_{file_counter}', 'texto_original': text,
                                            'storage_mode': 'new', 'ocr': ocr}
                                    item_to_database([item], 'texts')
                                else:
                                    print(f'We may need and OCR {url}')
                        elif rarfile_doc_format.lower() not in non_parsing_doc_formats:
                            print(f'Unstudied format inside rar {rarfile_doc_format}')
            return
    else:
        print(f'{doc_format}: {url}')
        return
    text, ocr = get_pdf_text(to_parse, url)
    if text:
        item = {'bid_id': bid_id, 'texto_original': text, 'storage_mode': 'new', 'ocr': ocr}
        item_to_database([item], 'texts')

def get_pdf_text(buffer, url):
    while True:
        try:
            parsed_data = parser.from_buffer(buffer)
            break
        except:
            continue
    ocr = False
    text = parsed_data['content']
    if 'content' in parsed_data and parsed_data['content'] is not None:
        text = remove_punctuation(text)
    if ('content' in parsed_data and parsed_data['content'] is None) or not text:
        headers = {'X-Tika-PDFextractInlineImages': 'true'}
        parsed_data = parser.from_buffer(buffer, headers=headers)
        ocr = True
        text = parsed_data['content']
        if parsed_data['content'] is None or not text:
            print(f"OCR couldn't extract text from {url}")
            return '', False
        else:
            text = remove_punctuation(text)
    tb_text = TextBlob(text)
    lg = tb_text.detect_language()
    if lg != 'es':
        print(f"Detected non-spanish text in document {url}. {lg} detected")
        return '', False
    # else:
    #     pos_and_lemmatize(text)
    return text, ocr

def remove_punctuation(raw_text):
    """Process raw text to perform a first clean to improve language detection in later steps

    :param raw_text: Raw text extracted from document
    :return:
    """
    try:
        tokens = [token for token in nltk.word_tokenize(raw_text, 'spanish') if token.isalnum()]
    except BaseException as e:
        print('Error tokenizing')
    return ' '.join(tokens)

def pos_and_lemmatize(text):
    tokens = text.split()
    split_tokens = split_array(tokens,
                               150)  # Split text in chunks of 150 words so librairy can lemmatize the whole text
    lemmatized_text = str()
    for chunk in split_tokens:
        chunk = ' '.join(chunk).lower()
        json_request = {"filter": ["NOUN", "ADJECTIVE", "VERB", "ADVERB"], "multigrams": True, "references": False,
                        "text": chunk}
        token_info = requests.post('http://localhost:7777/es/annotations', json=json_request).content.decode(
            'utf-8')
        token_info = json.loads(token_info)['annotatedText']
        print('stop')
