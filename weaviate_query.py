import weaviate
from sentence_transformers import SentenceTransformer

client = weaviate.Client("http://localhost:8080")

model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2', device='cpu')
# model = SentenceTransformer('camembert/camembert-base', device='cpu')

text = input("Type something to test this out: ")
embedding = model.encode([text])

query = f"""
{{
    Get {{
    Sentence (
      limit: 100
      nearVector: {{
        vector: {str(embedding[0])}
      }}
    ) {{
      docid
      text 
      sentid
      hasAuthors {{ 
        ...on Author {{
          identifier
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

results = client.query.raw(query)

minimal_distance = None
maximal_distance = None
for result in results['data']['Get']['Sentence']:
    distance = float(result['_additional']['distance'])
    if minimal_distance is None:
        minimal_distance = distance
    else:
        maximal_distance = distance
    if distance >= 2 * minimal_distance:
        break


def normalise(distance):
    return max(0, (maximal_distance - distance) / (maximal_distance - minimal_distance))


scores = {}
texts = {}

for result in results['data']['Get']['Sentence']:
    distance = result['_additional']['distance']
    text = result['text']
    print(f"{distance} ({result['docid']}) -> {text}")
    if result['hasAuthors'] is None:
        continue
    for author in result['hasAuthors']:
        author_identifier = author['identifier']
        if not author_identifier in scores.keys():
            scores[author_identifier] = 0
            texts[author_identifier] = []
        scores[author_identifier] += normalise(float(distance))
        texts[author_identifier].append(text)

authors_list = sorted([(v, k) for k, v in scores.items()], reverse=True)

for author in authors_list:
    print("*************************************************")
    print(f"{author[1]} ({author[0]})")
    print(texts[author[1]])
