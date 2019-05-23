from gensim import corpora
from gensim import models
from topic_model.text_preprocessor import preprocess_corpus
from topic_model.text_preprocessor import join_long_docs
from config import get_db_connection
import os


def model_generator():

    df = get_db_connection().readDBtable(tablename='docs', selectOptions='Expediente_Licitacion, Texto_Pliego')
    # TODO: Necesito el nombre de la licitacion para concatenar documentos que son demasiado largos
    bids = df['Expediente_Licitacion'].tolist()
    texts = df['Texto_Pliego'].tolist()

    corpus = join_long_docs(bids, texts)

    preprocessed_corpus = preprocess_corpus(corpus)

    dictionary = corpora.Dictionary(preprocessed_corpus)
    # Importante comprender la info que da esto
    corpus_bow = [dictionary.doc2bow(doc) for doc in preprocessed_corpus]
    tfidf_model = models.TfidfModel(corpus_bow)
    corpus_tfidf = tfidf_model[corpus_bow]
    pass