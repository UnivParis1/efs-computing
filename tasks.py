import weaviate
from celery import Celery
from celery.signals import worker_process_init
from dotenv import dotenv_values

from sentence_transformers import SentenceTransformer

MIN_PRECISION = 1.1

celery_params = dict(dotenv_values(".env.celery"))

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


def build_query(embedding):
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
def find_experts(sentence, precision):
    client = weaviate.Client("http://localhost:8080")

    embedding = initialization.model.encode([sentence])

    results = client.query.raw(build_query(embedding[0]))

    scores, texts, names = compute_scores_by_author(results, max(MIN_PRECISION, float(precision)))

    return format_output(scores, texts, names)
