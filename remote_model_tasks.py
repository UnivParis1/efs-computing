import openai
from celery import Celery
from dotenv import dotenv_values

from scoring_strategy import ScoringStrategy
from vector_database import VectorDatabase

EMBEDDING_MODEL = 'text-embedding-ada-002'

celery_params = dict(dotenv_values(".env.celery"))
openai_params = dict(dotenv_values(".env.openai"))

openai.organization = openai_params['organization']
openai.api_key = openai_params['api_key']

app = Celery('remote_model_tasks', **celery_params)


def get_openai_embedding(text_or_tokens, model=EMBEDDING_MODEL):
    return openai.Embedding.create(input=text_or_tokens, model=model)["data"][0]["embedding"]


@app.task(name='remote_model_tasks.find_expert_with_ada')
def find_experts(sentence, precision):
    sentence_class = "AdaSentence"
    embedding = f"[{' '.join(map(str, get_openai_embedding(sentence)))}]"
    results = VectorDatabase().results(embedding, sentence_class)
    return ScoringStrategy().compute_scores_by_author(results['data']['Get'][sentence_class], precision)
