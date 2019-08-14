#-*- coding: utf-8 -*-
import json
import re
import unicodedata

import nltk
import requests
from nltk.corpus import stopwords

from setup.config import IRRELEVANT_STRINGS, text_logger, CUSTOM_STOPWORDS, EQUIVALENCES
from helpers.helpers import split_array


def clear_irrelevant_strings(raw_text):
    for str in IRRELEVANT_STRINGS:
        raw_text = raw_text.replace(str, '')
    return raw_text


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

def clean_text(raw_text):
    no_punctuation = remove_punctuation(raw_text)
    clean = clear_irrelevant_strings(no_punctuation)
    clean = remove_punctuation(clean)
    return clean


def pos_lemmatize_and_ngams(raw_text):
    text_logger.debug('Starting lemmatization and pos tagging...')
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

def remove_stopwords(tokens):
    text_logger.debug('Removing stopwords...')
    try:
        sw = stopwords.words('spanish')
    except:
        # Download stopwords
        nltk.download('stopwords')
        sw = stopwords.words('spanish')
    # Remove stopwords
    tokens = [token for token in tokens if len(token) > 2 and token not in sw and re.search('\d', token) is None]
    
    return tokens

def remove_irrelevant_tokens(tokens):
    """Remove tokens that have no vowels in it and tokens in list of irrelevant
       words
    """
    vowels = "aeiouAEIOU"
    tokens = [token for token in tokens if token not in CUSTOM_STOPWORDS]
    tokens = [token for token in tokens if any(char in vowels for char in token)]
    return tokens

def remove_accents(text):
    """Function to remove all accents in a text except for character ñ
    """

    good_accents = {
    u'\N{COMBINING TILDE}'
    }


    text = ''.join(c for c in unicodedata.normalize('NFKD', text)
                       if (unicodedata.category(c) != 'Mn'
                           or c in good_accents))
    return text



def apply_equivalences(tokens):
    for index, token in enumerate(tokens):
        if token in EQUIVALENCES:
            tokens[index] = EQUIVALENCES[token]
    return tokens