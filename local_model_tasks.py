from celery import Celery
from celery.signals import worker_process_init
from dotenv import dotenv_values
from sentence_transformers import SentenceTransformer

from scoring_strategy import ScoringStrategy
from vector_database import VectorDatabase

celery_params = dict(dotenv_values(".env.celery"))
weaviate_params = dict(dotenv_values(".env.weaviate"))

EMBEDDING_MODEL = 'text-embedding-ada-002'
EMBEDDING_CTX_LENGTH = 8191
EMBEDDING_ENCODING = 'cl100k_base'

app = Celery('local_model_tasks', **celery_params)


def initialization():
    initialization.model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2',
                                               device='cpu')


@worker_process_init.connect()
def setup(**kwargs):
    print('initializing SBert sentence embedding model')
    initialization()
    print('done initializing SBert sentence embedding model')


@app.task(name='local_model_tasks.find_expert_with_sbert')
def find_experts(sentence, precision):
    sentence_class = "SbertSentence"
    embedding = initialization.model.encode([sentence])[0]
    results = VectorDatabase().results(embedding, sentence_class)
    return ScoringStrategy().compute_scores_by_author(results['data']['Get'][sentence_class], precision)
