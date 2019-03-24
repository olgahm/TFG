from nltk.corpus import stopwords
import traceback
import subprocess
from KafNafParserPy import KafNafParser
from unidecode import unidecode
import threading
from threading import Thread, Lock
import multiprocessing
import re
import os
from datetime import datetime


def get_lemmatized_tokens(text, naf_filename):
    # print(naf_filename)
    try:
        text = ' '.join(
            [word.lower() for word in text.split() if not any(char.isdigit() for char in word) and len(word) > 2])
        # text = unidecode(text.lower())  # Eliminar tildes para facilitar tokenizacion y homogeneización
        process = subprocess.Popen(
            ['powershell', (f"'{text}' | java -jar ixa-pipes/ixa-pipe-tok-1.8.5-exec.jar tok -l es | java -jar "
                            f"ixa-pipes/ixa-pipe-pos-1.5.1-exec.jar tag -m "
                            f"ixa-pipes/morph-models-1.5.0/es/es-pos-perceptron-autodict01-ancora-2.0.bin -lm "
                            f"ixa-pipes/morph-models-1.5.0/es/es-lemma-perceptron-ancora-2.0.bin")],
            stdout=subprocess.PIPE)
        naf_doc = process.communicate()
        with open(naf_filename, 'wb') as naf_file:
            naf_file.write(naf_doc[0])
        naf_parser = KafNafParser(naf_filename)
        lemmatized_tokens = list()
        for term in naf_parser.get_terms():
            lemmatized_tokens.append(term.get_lemma())
        os.remove(naf_filename)
        return lemmatized_tokens
    except:
        # print(traceback.format_exc())
        split_text = text.split()
        lemmatized_tokens_1 = get_lemmatized_tokens(" ".join(split_text[:len(split_text) // 2]), f'{naf_filename}_1')
        lemmatized_tokens_2 = get_lemmatized_tokens(" ".join(split_text[len(split_text) // 2:]), f'{naf_filename}_2')
        return lemmatized_tokens_1 + lemmatized_tokens_2


def homogenize(tokens):
    return [word.lower() for word in tokens if word.isalnum()]


def clean(tokens):
    sw = [unidecode(sw) for sw in stopwords.words('spanish')]
    clean_tokens = [unidecode(token) for token in tokens if token not in sw and len(token) > 2]
    return clean_tokens


def preprocess_text(text, naf_filename):
    tokens = get_lemmatized_tokens(text, naf_filename)
    homogenized = homogenize(tokens)
    cleaned = clean(homogenized)
    return cleaned


def join_long_docs(bids, texts):
    """Function to analyze all rows in the docs database and join rows corresponding to documents too long to be
    stored in the same row of the table

    :param bids: List of stored bids
    :param texts: List of texts corresponding to each bid
    :return:
    """
    joined_texts = list()
    joined_docs = dict()
    for index, bid in enumerate(bids):
        if re.search('(.*?)(_[12])', bid):
            bid_prefix = re.search('(.*?)(_[12])', bid).group(1)
            if joined_docs.get(bid_prefix, ''):
                joined_docs[bid_prefix] += f' {texts[index]}'
            else:
                joined_docs[bid_prefix] = texts[index]
        else:
            joined_docs[bid] = texts[index]

    for doc in joined_docs:
        joined_texts.append(joined_docs[
                                doc])  # if '' in joined_docs[doc]:  #     print(f"delete from docs where
        # Expediente_Licitacion='{doc}';")

    return joined_texts


def preprocess_corpus_chunk(chunk, q, thread_id):
    """Preprocess all documents in a given chunk extracted from the main corpus

    :param chunk: Array of documment texts
    :return:
    """

    docs = list()
    for index, doc in enumerate(chunk):
        naf_filename = f'NAF_{thread_id}_{index + 1}'
        # mutex.acquire()
        text = preprocess_text(doc, naf_filename)
        docs.append(text)

    q.put(docs)
    return None


def preprocess_corpus(corpus):
    """Iterate over all documents in input corpus and keep only relevant information

    :param corpus: Array of document texts
    :return:
    """
    start_time = datetime.now()
    manager = multiprocessing.Manager()
    preprocessed_corpus = list()
    jobs = []
    corpus_chunks = split(corpus, len(corpus) // 5)
    process_id = 0
    q = multiprocessing.Queue()
    for chunk in corpus_chunks:
        process_id += 1
        p = multiprocessing.Process(target=preprocess_corpus_chunk, args=[chunk, q, process_id])
        jobs.append(p)
        p.start()
    print('All processes started')
    for i in range(len(jobs)):
        preprocessed_corpus += q.get()
    for proc in jobs:
        proc.join()

    end_time = datetime.now()
    print(start_time, end_time, f'{5} procesos')

    with open("corpus.json", "w") as corpus_file:
        corpus_file.write(str(preprocessed_corpus))
    return preprocessed_corpus


def split(arr, size):
    arrs = []
    while len(arr) > size:
        pice = arr[:size]
        arrs.append(pice)
        arr = arr[size:]
    arrs.append(arr)
    return arrs
