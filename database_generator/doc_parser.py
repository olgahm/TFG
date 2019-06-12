import json
from time import sleep
from config import content_types_to_parse
from config import split_array

import nltk
from nltk.corpus import stopwords
import requests
from textblob import TextBlob
from tika import parser
from langdetect import detect

from config import get_db_connection
from database_generator.document_format_analyzer import format_parsing
from database_generator.db_helpers import item_to_database
import sys


def start_text_extraction(urls):
    db_conn = get_db_connection()
    for url in urls:
        doc_to_tokens(url, db_conn)


def doc_to_tokens(url, db_conn):
    documents = url_to_doc(url)
    if documents is not None:
        for document in documents:
            tokens, lang, ocr = extract_text(document)
            doc = [{'doc_url': url, 'idioma': lang, 'ocr': ocr, 'storage_mode': 'update'}]
            item_to_database(db_conn, doc, 'docs', 'doc_url')
            text = [{'doc_url': url, 'tokens': tokens}]
            item_to_database(db_conn, text, 'texts')
            # item = {'doc_url': url, 'tokens': tokens}
            # item_to_database(db_conn, i)
    else:
        print(url)


def url_to_doc(url):
    response = requests.get(url)
    content_type = response.headers['Content-Type'].split(';')[0]
    if content_type in content_types_to_parse:
        format_function = format_parsing.get(content_type)
        if format_function is not None:
            return format_function(response.content)
        else:
            return [response.content]
    else:
        print('Content type not considered')


def extract_text(buffer):
    lang = str()
    ocr = False
    while True:
        try:
            parsed_data = parser.from_buffer(buffer)
            break
        except RuntimeError:
            print('Error when starting tika server. Retrying...')
        except BaseException as e:
            print(type(e).__name__)
            sleep(30)
    text = parsed_data['content']
    if 'content' in parsed_data and parsed_data['content'] is not None:
        text = remove_punctuation(text)
    if ('content' in parsed_data and parsed_data['content'] is None) or not text:
        headers = {'X-Tika-PDFOcrStrategy': 'ocr_only', 'X-Tika-OCRLanguage': 'spa'}
        parsed_data = parser.from_buffer(buffer, headers=headers)
        ocr = True
        text = parsed_data['content']
        if parsed_data['content'] is None or not text:
            return None, ocr, False
        else:
            text = remove_punctuation(text)
    tb_text = TextBlob(text)
    lg = tb_text.detect_language()
    lg_lg = detect(text)
    if lg != lg_lg:
        print('Not equal')
    if lg != 'es':
        print(f"{lg} detected")
        return '', False
    else:
        tokens = pos_and_lemmatize(text)
        tokens = clean_tokens(tokens)
    return ' '.join(tokens), lg, ocr


def remove_punctuation(raw_text):
    """Process raw text to perform a first clean to improve language detection in later steps

    :param raw_text: Raw text extracted from document
    :return:
    """
    try:
        tokens = [token for token in nltk.word_tokenize(raw_text, 'spanish') if token.isalnum()]
    except:
        # Download punkt module from nltk
        nltk.download('punkt')
        tokens = [token for token in nltk.word_tokenize(raw_text, 'spanish') if token.isalnum()]
    return ' '.join(tokens)


def pos_and_lemmatize(raw_text):
    tokens = list()
    # Split document in chunks of 80 words in order to lemmatize each chunk with librairy
    text_chunks = split_array(raw_text.split(), 100)
    for text in text_chunks:
        tokens_chunk = list()
        json_request = {"filter": ["NOUN", "ADJECTIVE", "VERB", "ADVERB"], "multigrams": True, "references": False,
                        "lang": 'es', "text": ' '.join(text).lower()}
        token_info = requests.post('http://localhost:7777/nlp/annotations', json=json_request).content.decode('utf-8')
        token_info = json.loads(token_info)['annotatedText']
        for token in token_info:
            tokens_chunk.append(token['token']['lemma'])
        tokens += tokens_chunk
    return tokens


def clean_tokens(tokens):
    try:
        sw = stopwords.words('spanish')
    except:
        # Download stopwords
        nltk.download('stopwords')
        sw = stopwords.words('spanish')
    # Remove stopwords
    tokens = [token for token in tokens if token not in sw]
    return tokens
