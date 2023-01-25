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
MIN_PRECISION = 1.1

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
          hasAuthors {{ 
            ...on Author {{
              identifier
              name
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
          hasAuthors {{ 
            ...on Author {{
              identifier
              name
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


def compute_distances(results, precision):
    minimal_distance = None
    maximal_distance = None
    for result in results['data']['Get']['Sentence']:
        if result['hasAuthors'] is None:
            continue
        distance = float(result['_additional']['distance'])
        if minimal_distance is None:
            minimal_distance = distance
        maximal_distance = distance
        if distance >= precision * minimal_distance:
            break
    print(f">>>>>>>>>>>>> {str(minimal_distance)} {str(maximal_distance)} {str(precision)}")
    return maximal_distance, minimal_distance


def compute_scores_by_author(results, precision):
    maximal_distance, minimal_distance = compute_distances(results, precision)
    scores = {}
    texts = {}
    names = {}
    for result in results['data']['Get']['Sentence']:
        distance = result['_additional']['distance']
        text = result['text']
        # print(f"{distance} ({result['docid']}) -> {text}")
        if result['hasAuthors'] is None:
            continue
        for author in result['hasAuthors']:
            author_identifier = author['identifier']
            if author_identifier not in scores.keys():
                scores[author_identifier] = 0
                texts[author_identifier] = []
                names[author_identifier] = author['name']
            if text not in texts[author_identifier]:
                scores[author_identifier] += normalise(float(distance), minimal_distance, maximal_distance)
                texts[author_identifier].append(text)
    return scores, texts, names


def format_output(scores, texts, names):
    print(scores)
    authors_list = sorted([(v, k) for k, v in scores.items() if v > 0], reverse=True)
    print(authors_list)
    output = []
    for author in authors_list:
        output.append(
            {'identifier': author[1], 'name': names[author[1]], 'score': author[0], 'texts': texts[author[1]]}
        )
    return output


@app.task
def find_experts(sentence, precision, model=DEFAULT_MODEL):
    client = weaviate.Client("http://localhost:8080")
    print(f"Requested model : {model}")
    if model == 'ada':
        embedding = f"[{' '.join(map(str, get_openai_embedding(sentence)))}]"
    else:
        embedding = initialization.model.encode([sentence])[0]
    results = client.query.raw(build_query(embedding, model))
    scores, texts, names = compute_scores_by_author(results, max(MIN_PRECISION, float(precision)))

    return format_output(scores, texts, names)
