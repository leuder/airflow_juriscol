import json
from collections import Counter
from datetime import timedelta

import spacy
import pickle
from airflow import DAG
from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from bson.objectid import ObjectId
from tqdm import tqdm
import numpy as np
import os


def retrive_sentece_text(query={}):
    mongo_hook = MongoHook()
    sentence_text = mongo_hook.find('sentence', query, projection={
                                    'text': 1}, mongo_db='juriscol', batch_size=256)
    mongo_hook.close_conn()
    return sentence_text, sentence_text.count()


def chunk(cursor, size):
    start = 0
    end = size
    document = [d for d in cursor[start:end]]
    while len(document):
        yield document
        start = end
        end += size
        document = cursor[start:end]


def insert_pos():
    mongo_hook = MongoHook()
    sentence_list, total = retrive_sentece_text({"vocabulary": {"$exists": 0}})
    nlp = spacy.load('es_core_news_sm',  disable=["ner"])
    nlp.max_length = 5621760
    pbar = tqdm(total=total, desc="PoS")
    for sentence in sentence_list:
        doc = nlp(sentence["text"])
        # vocabulary with sentence id
        mongo_hook.update_one(
            'sentence',
            {"_id": sentence["_id"]},
            {"$set": {"vocabulary": 1}},
        )

        pos_vocabulary = [{"sentence": sentence["_id"], **pos}
                          for pos in get_pos_list(doc)]

        mongo_hook.insert_many('vocabulary',  pos_vocabulary)
        pbar.update(1)

    pbar.close()
    mongo_hook.close_conn()


def insert_ner():
    sentence_list, total = retrive_sentece_text({
        'collective': {
            '$exists': 0
        }
    })
    nlp = spacy.load('es_core_news_sm', disable=["tagger", "parser"])
    nlp.max_length = 5621760
    mongo_hook = MongoHook()
    pbar = tqdm(total=total)
    # batch_size = 800
    for sentence in sentence_list:
        doc = nlp(sentence["text"])

        # control record
        mongo_hook.update_one(
            'sentence',
            {"_id": sentence["_id"]},
            {"$set": {"collective": 1}})

        ner_sentence = [{"sentence": sentence["_id"], **entity}
                        for entity in entities(doc)]

        mongo_hook.insert_many('collective',  ner_sentence)

        pbar.update(1)
    pbar.close()
    mongo_hook.close_conn()


def insert_topic():

    dir_path = os.path.dirname(os.path.realpath(__file__))

    with open(os.path.join(dir_path, 'bow.pickle'), 'rb') as f:
        bow = pickle.load(f)

    with open(os.path.join(dir_path, 'lda.pickle'), 'rb') as f:
        lda = pickle.load(f)

    pipleline = [
        {
            '$project': {
                'text': 1,
                'is_alpha': 1,
                'is_stop': 1,
                'sentence': 1,
                'length': {
                    '$strLenCP': '$text'
                }
            }
        }, {
            '$match': {
                'is_alpha': True,
                'is_stop': False,
                'length': {
                    '$gt': 2
                }
            }
        }, {
            '$group': {
                '_id': '$sentence',
                'words': {
                    '$push': '$text'
                }
            }
        }
    ]

    mongo_hook = MongoHook()
    sentence_list = mongo_hook.aggregate('vocabulary', pipleline)

    for sentence in sentence_list:
        X_bow = bow.transform([' '.join(sentence['words'])])
        lda_round = np.round(lda.transform(X_bow), 2)
        _, indices = np.where(lda_round > 0)
        topic_sentence = dict()
        topic_sentence = [
            {"porcentaje": lda_round[0][i], "name":f"Topic {i}"}
            for i in indices]

        mongo_hook.update_one(
            'sentence',
            {"_id": sentence["_id"]},
            {"$set": {"lda_topics": topic_sentence}})


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('juriscol_pos',
          default_args=default_args,
          description='Juriscol PoS data')

ner_task = PythonOperator(
    task_id="ner_task_txt",
    python_callable=insert_ner,
    dag=dag,
)

pos_task = PythonOperator(
    task_id="pos_task_txt",
    python_callable=insert_pos,
    dag=dag,
)

topic_task = PythonOperator(
    task_id="topic_task_txt",
    python_callable=insert_topic,
    dag=dag,
)

ner_task, pos_task >> topic_task


def get_pos_list(doc):
    """Return json array tag pos"""
    def map_POS(token):
        return {
            "text": token.text,
            "lemma": token.lemma_,
            "pos": token.pos_,
            "tag": token.tag_,
            "dep_": token.dep_,
            "shape_": token.shape_,
            "is_alpha": token.is_alpha,
            "is_stop": token.is_stop,
            "sentiment": token.sentiment
        }

    for token in doc:
        yield map_POS(token)


def entities(doc):
    """Return json array tag pos"""
    def map_entity(token):
        return {
            "text": token.text,
            "label": token.label_
        }

    for token in doc.ents:
        yield map_entity(token)
