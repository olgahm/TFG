from time import sleep

import requests
from requests import HTTPError
from textblob import TextBlob
from tika import parser

from helpers import get_db_connection, split_array, insert_or_update_records
from config import text_logger, FINAL_CONTENT_TYPES_TO_PARSE
from text_extractor.format_processor import content_type_parsing_functions, html_processing_by_domain_map
from urllib.parse import urlparse

from vocabulary_generator.text_cleaner import clean_text, pos_lemmatize_and_ngams, remove_stopwords
import sys
import traceback


def start_text_extraction(urls):
    """Function to init database connection for every process in the pool and
       trigger url processing for text extraction
    """
    text_logger.debug('Starting DB connection...')
    db_conn = get_db_connection()
    for url in urls:
        doc_to_tokens(url, db_conn)

def split_token_array(tokens):
    """Recursive function to split long texts in halves
       This way, when a document text is too long to fit in a single row 
       in the database, we split it in half recursively until the halves fit
       In this cases there is more than one row holding the token list of a single 
       document.

        Params:
            tokens: Array of tokens
        Outputs:
            final_token_list: List of strings. Each string contains the tokens in 
                              string format
    """
    final_token_list = list()
    # Split array of tokens in two
    token_chunks = [tokens[:len(tokens)//2]] + [tokens[len(tokens)//2:]]
    # Transform tokens lists into text for obtaining the total length of the string 
    # that would be stored in the database
    token_chunks_str_0 = ' '.join(token_chunks[0])
    token_chunks_str_1 = ' '.join(token_chunks[1])
    # We consider 50000 characters as the maximum string length we are considering
    # If this length is exceded, call this function recursively for both halves
    if len(token_chunks_str_0) > 50000:
        token_chunks[0] = split_token_array(token_chunks[0])
    else:
        final_token_list.append(token_chunks_str_0)
    if len(token_chunks_str_1) > 50000:
        token_chunks[1] = split_token_array(token_chunks[1])
    else:
        final_token_list.append(token_chunks_str_1)
    return final_token_list


def doc_to_tokens(url, db_conn):
    text_logger.debug(f'Starting extraction process for {url}...')
    documents = url_to_doc(url, db_conn)
    if documents is not None and documents:
        text_logger.debug('At least one document found. Trying to store any text...')
        texts = list()
        for document in documents:
            tokens, lang, ocr = extract_text(url, document)
            if tokens:
                text_logger.debug('Extracted tokens. Updating doc info...')
                doc = [{'doc_url': url, 'idioma': lang, 'ocr': ocr, 'storage_mode': 'update'}]
                insert_or_update_records(db_conn, doc, 'docs', 'doc_url')
                if len(tokens) > 50000:
                    token_chunks = split_token_array(tokens.split())
                    # print(token_chunks)
                    # sys.exit(0)
                else:
                    token_chunks = [tokens]
                for tokens in token_chunks:
                    texts.append({'doc_url': url, 'tokens': tokens})
            elif lang:
                text_logger.debug('Non spanish document. Updating doc info...')
                doc = [{'doc_url': url, 'idioma': lang, 'ocr': ocr, 'storage_mode': 'update'}]
                insert_or_update_records(db_conn, doc, 'docs', 'doc_url')
        if texts:
            text_logger.debug('Storing text in database...')
            insert_or_update_records(db_conn, texts, 'texts')


def url_to_doc(url, db_conn):
    parsed_uri = urlparse(url)
    domain = f'{parsed_uri.scheme}://{parsed_uri.netloc}/'
    if domain in html_processing_by_domain_map['ignore']:
        text_logger.debug(f'Ignoring {url}')
        return
    try:
        response = requests.get(url)
        response.raise_for_status()
        text_logger.debug(f'Successful download for {url}')
    except requests.exceptions.SSLError:
        text_logger.error(f'SSL error when downloading {url}')
        return
    except HTTPError:
        text_logger.error(f'Error {response.status_code} for url {url}. Deleting entry from docs table...')
        # print(f'Error {response.status_code} for url {url}. Deleting entry from docs table...')
        db_conn.deleteRowTables('docs', f"doc_url='{url}'")
        return
    content_type = response.headers['Content-Type'].split(';')[0]
    if content_type in FINAL_CONTENT_TYPES_TO_PARSE:
        format_function = content_type_parsing_functions.get(content_type)
        if format_function is not None:
            return format_function(url, response, db_conn)
        else:
            return [response.content]
    else:
        print(f'Content type {content_type} not considered. URL: {url}')
        text_logger.debug(f'Content type {content_type} not considered. URL: {url}')


def extract_text(url, buffer):
    text_logger.debug('Starting text extraction...')
    ocr = False
    while True:
        try:
            tika_resp = parser.from_buffer(buffer)
            break
        except RuntimeError:
            print('Error when starting tika server. Retrying...')
        except BaseException as e:
            print('Tika exception')
            print(type(e).__name__)
            sleep(30)
    text = str()
    if tika_resp.get('content') is not None:
        """ 
        In some cases Tika can extract text from a PDF that should be parsed by OCR
        To check if the text has been properly extracted, clean text by removing 
        punctuation and irrelevant strings.
        """
        text = tika_resp['content']
        text = clean_text(text)
    # If tika returned no text or the text is empty or too short after cleanup:
    if tika_resp.get('content') is None or not text or len(text.split()) <= 2:
        text_logger.debug('No text could be extracted with basic processing.' \
                           'Appliying OCR to buffer by rendering pages as images...')
        """
        Tika setup for using OCR to extract text. This OCR mechanism considers 
        every page in the PDF as an image and performs character recognition to every 
        page. Tipically, this mechanism is faster for larger PDFs
        """
        headers = {'X-Tika-PDFOcrStrategy': 'ocr_and_text', 'X-Tika-OCRLanguage': 'spa'}
        tika_resp = parser.from_buffer(buffer, headers=headers)
        ocr = True
        text = str()
        # Text cleanup
        if tika_resp.get('content') is not None:
            text = tika_resp['content']
            text = clean_text(text)
        # Check if text after cleanup
        if tika_resp.get('content') is None or not text or len(text.split()) <= 2:
            # Sometimes this first OCR approach fails
            if tika_resp.get('status') == 422:
                print(url)
                """
                Tika setup for OCR. This mechanism performs OCR to all in line images
                individually
                """    
                headers = {'X-Tika-PDFExtractInlineImages': 'true', 'X-Tika-OCRLanguage': 'spa'}
                tika_resp = parser.from_buffer(buffer, headers=headers)
                ocr = True
                text = str()
                if tika_resp.get('content') is not None:
                    text = tika_resp['content']
                    text = clean_text(text)
                if tika_resp.get('content') is None or not text or len(text.split()) <= 2:
                    text_logger.debug('No text could be extracted after OCR processing')
                    return '', '', False
            if not text or len(text.split()) <= 2:
                text_logger.debug('No text could be extracted after OCR processing')
                return '', '', False
    tb_text = TextBlob(text)
    try:
        lg = tb_text.detect_language()
    except:
        text_logger.error('Exception caught when detecting language')
        return '', 'unk', ocr
    tokens = list()
    if lg == 'es':
        text_logger.debug('Successful text extraction of spanish text. Obtaining tokens...')
        tokens = pos_lemmatize_and_ngams(text)
        tokens = remove_stopwords(tokens)
        # print('Got tokens')
    else:
        text_logger.debug('Non-spanish text')
        # print('Not spanish')
        # print(lg)

    # print(tokens)
    return ' '.join(tokens), lg, ocr
