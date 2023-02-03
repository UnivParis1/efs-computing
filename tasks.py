import openai
import weaviate
from celery import Celery
from celery.signals import worker_process_init
from dotenv import dotenv_values

from sentence_transformers import SentenceTransformer

DEFAULT_MODEL = "sbert"

celery_params = dict(dotenv_values(".env.celery"))
openai_params = dict(dotenv_values(".env.openai"))

EMBEDDING_MODEL = 'text-embedding-ada-002'
EMBEDDING_CTX_LENGTH = 8191
EMBEDDING_ENCODING = 'cl100k_base'

openai.organization = openai_params['organization']
openai.api_key = openai_params['api_key']
MIN_PRECISION = 0.05
MAX_PRECISION = 0.7

app = Celery('tasks', **celery_params)


def initialization():
    proxies = dict(dotenv_values(".env.proxies"))

    initialization.model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2',
                                               device='cpu')


@worker_process_init.connect()
def setup(**kwargs):
    print('initializing sentence embedding model')
    initialization()
    print('done initializing sentence embedding model')


def build_query_with_model_filter(embedding, model):
    return f"""
    {{
        Get {{
        Sentence (
          limit: 100
          where: {{
            path: ["model"],
            operator: Equal,
            valueString: "{model}"
            }}
          nearVector: {{
            vector: {str(embedding)}
          }}
        ) {{
          docid
          text 
          sentid
          hasPublication {{ 
            ...on Publication {{
              doc_type
              docid
              fr_title
              en_title
              fr_abstract
              en_abstract
              fr_keyword
              en_keyword
              citation_ref
              citation_full
              hasAuthors {{
                ...on Author {{
                  identifier
                  name
                }}
              }}
            }}
          }}
          _additional {{
            distance
            certainty
          }}
        }}
      }}
    }}
    """


def build_query(embedding, model):
    return f"""
        {{
            Get {{
            Sentence (
              limit: 100
              nearVector: {{
                vector: {str(embedding)}
              }}
            ) {{
              docid
              text 
              sentid
              hasPublication {{ 
                ...on Publication {{
                  doc_type
                  docid
                  fr_title
                  en_title
                  fr_abstract
                  en_abstract
                  fr_keyword
                  en_keyword
                  citation_ref
                  citation_full
                  hasAuthors {{
                    ...on Author {{
                      identifier
                      name
                      own_inst
                    }}
                  }}
                }}
              }}
              _additional {{
                distance
                certainty
              }}
            }}
          }}
        }}
        """


def get_openai_embedding(text_or_tokens, model=EMBEDDING_MODEL):
    return openai.Embedding.create(input=text_or_tokens, model=model)["data"][0]["embedding"]


def normalise(distance, minimal_distance, maximal_distance):
    return max(0, (maximal_distance - distance) / (maximal_distance - minimal_distance))


def compute_score(distance, precision):
    return (precision - distance) / precision


def compute_scores_by_author(results, precision):
    inverted_results = {}
    for sent in results['data']['Get']['Sentence']:
        distance = sent['_additional']['distance']
        if distance > precision:
            continue
        sent_score = compute_score(distance, precision)
        docid = sent['docid']
        sentid = sent['sentid']
        sent_data = {'text': sent['text'], 'score': sent_score, 'id': sentid}
        print(f"{distance} ({docid}) -> {sent['text']} (score : {sent_score}")
        if sent['hasPublication'] is None:
            continue
        pub = sent['hasPublication'][0]
        pub_data = {key: str(pub[key]) if pub[key] is not None else '' for key in
                    ['citation_full', 'citation_ref', 'doc_type', 'docid', 'en_abstract', 'en_keyword', 'en_title',
                     'fr_abstract', 'fr_keyword', 'fr_title']}
        if pub['hasAuthors'] is None:
            continue
        for auth in pub['hasAuthors']:
            author_identifier = auth['identifier']
            auth_data = {key: str(auth[key]) if auth[key] is not None else '' for key in
                         ['identifier', 'name', 'own_inst']}
            if author_identifier not in inverted_results.keys():
                inverted_results[author_identifier] = auth_data | {'pubs': {}, 'score': 0, 'max_score': 0,
                                                                   'avg_score': 0, 'scores_for_avg': [],
                                                                   'min_dist': 2.0,
                                                                   'avg_dist': 0, 'dist_for_avg': []}
            if docid not in inverted_results[author_identifier]['pubs'].keys():
                inverted_results[author_identifier]['pubs'][docid] = pub_data | {'score': 0, 'sents': {}}
            inverted_results[author_identifier]['pubs'][docid]['score'] += sent_score
            inverted_results[author_identifier]['score'] += sent_score
            inverted_results[author_identifier]['max_score'] = max(sent_score,
                                                                   inverted_results[author_identifier]['max_score'])
            inverted_results[author_identifier]['min_dist'] = min(distance,
                                                                  inverted_results[author_identifier]['min_dist'])
            inverted_results[author_identifier]['scores_for_avg'].append(sent_score)
            inverted_results[author_identifier]['dist_for_avg'].append(distance)
            inverted_results[author_identifier]['avg_scores'] = avg(
                inverted_results[author_identifier]['scores_for_avg'])
            inverted_results[author_identifier]['avg_dist'] = avg(inverted_results[author_identifier]['dist_for_avg'])
            if sentid not in inverted_results[author_identifier]['pubs'][docid]['sents'].keys():
                inverted_results[author_identifier]['pubs'][docid]['sents'][sentid] = sent_data | {'score': sent_score}
    return inverted_results


def avg(scores_list):
    return sum(scores_list) / len(scores_list)


def apply_limits(precision):
    return min(MAX_PRECISION, max(MIN_PRECISION, float(precision)))


@app.task
def find_experts(sentence, precision, model=DEFAULT_MODEL):
    client = weaviate.Client("http://localhost:8080")
    print(f"Requested model : {model}")
    if model == 'ada':
        embedding = f"[{' '.join(map(str, get_openai_embedding(sentence)))}]"
    else:
        embedding = initialization.model.encode([sentence])[0]
    results = client.query.raw(build_query(embedding, model))
    return compute_scores_by_author(results, apply_limits(precision))
