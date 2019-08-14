#-*- coding: utf-8 -*-

# from topic_modeler.text_preprocessor import preprocess_corpus
# from topic_modeler.text_preprocessor import join_long_docs
from helpers.helpers import get_db_connection, construct_dupes_dict
from setup.config import MALLET_BINARY_PATH
from gensim import corpora
from gensim.models import LdaModel
from gensim.models.wrappers import LdaMallet
from gensim.models.wrappers import ldamallet
from gensim.test.utils import datapath
from vocabulary_generator.text_cleaner import remove_accents, remove_irrelevant_tokens, apply_equivalences
import pyLDAvis.gensim
import sys

'español' == 'español'

def train_model(num_topics, documents):
    
    # documents = get_dictionary()
    dictionary = corpora.Dictionary(documents)
    max_tokens = len(dictionary.keys())
    # print(f'Num tokens before cleanup {len(dictionary.keys())}')
    dictionary.filter_extremes(no_below=10, no_above=0.7, keep_n=max_tokens)
    # print(f'Num tokens after cleanup {len(dictionary.keys())}')
    corpus_bow = [dictionary.doc2bow(doc) for doc in documents]
    mallet_model = LdaMallet(mallet_path=MALLET_BINARY_PATH, corpus=corpus_bow, id2word=dictionary, num_topics=num_topics)
    lda_model = ldamallet.malletmodel2ldamodel(mallet_model)
    return lda_model, corpus_bow, dictionary
    # Save model to file for loading from notebook


    # print('Printing topics')
    # print(lda.print_topics(num_topics=20, num_words=20))
    # print('Starting pyldavis')
    # visualization = pyLDAvis.gensim.prepare(lda, corpus_bow, dictionary)
    # pyLDAvis.show(visualization)
    # pyLDAvis.display(visualization)
    # print('pyldavis started')


def get_dictionary():
    db_conn = get_db_connection()

    df = db_conn.readDBtable(tablename='texts')

    doc_urls = df['doc_url'].tolist()
    tokens = df['tokens'].tolist()
    doc_dupes_dict = construct_dupes_dict(doc_urls)
    # print(doc_dupes_dict)
    documents = list()
    for doc in doc_dupes_dict:
        indices = doc_dupes_dict[doc]
        token_lists = [remove_accents(tokens[index]).replace('ñ', 'ñ').split() for index in indices]
        doc_tokens = [token for token_list in token_lists for token in token_list]
        doc_tokens = remove_irrelevant_tokens(apply_equivalences(doc_tokens))
        documents.append(doc_tokens)
    return documents

def load_model(model_name):
    file = datapath(f'/home/ohm/Documentos/TFG/models/{model_name}')
    return LdaModel.load(file)

def save_model(model, num_docs, num_topics):
    model_file = datapath(f'/home/ohm/Documentos/TFG/models/model_{num_docs}_{num_topics}')
    model.save(model_file)
