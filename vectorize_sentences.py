import itertools
import json
import os
import time
import uuid

import nltk
import openai
import pandas as pd
from sentence_transformers import SentenceTransformer

EMBEDDING_MODEL = 'text-embedding-ada-002'
EMBEDDING_CTX_LENGTH = 8191
EMBEDDING_ENCODING = 'cl100k_base'

openai.organization = "org-q7hqQnc3nXH91isXryTH6few"
# openai.api_key = os.getenv("OPENAI_API_KEY")
openai.api_key = "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"

sbert_model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2')

nltk.download('punkt')

csv = pd.read_csv(f"{os.path.expanduser('~')}/hal_p1_dump_json/hal_json.csv")
metadata = csv[['docid', 'fr_title_s', 'en_title_s', 'fr_abstract_s', 'en_abstract_s', 'authIdHal_s', 'authIdHal_i',
                'authLastNameFirstName_s']]

output_dir = f"{os.path.expanduser('~')}/hal_embeddings"
if os.path.exists(output_dir):
    os.rename(output_dir, f"{output_dir}-{time.strftime('%Y%m%d-%H%M%S')}")
os.mkdir(output_dir)


def split_into_sentences(text):
    if type(text) == str:
        return nltk.sent_tokenize(text)
    return []


def get_embedding(text_or_tokens, model=EMBEDDING_MODEL):
    return openai.Embedding.create(input=text_or_tokens, model=model)["data"][0]["embedding"]


metadata.loc[:, "texts_en"] = metadata.loc[:, "en_title_s"].map(lambda title: [title] if type(title) == str else []) + \
                              metadata.loc[:, "en_abstract_s"].map(split_into_sentences)
metadata.loc[:, "texts_fr"] = metadata.loc[:, "fr_title_s"].map(lambda title: [title] if type(title) == str else []) + \
                              metadata.loc[:, "fr_abstract_s"].map(split_into_sentences)
metadata.loc[:, "texts"] = metadata.loc[:, "texts_fr"] + metadata.loc[:, "texts_en"]

metadata.loc[:, "text_fr_concat"] = metadata.loc[:, "texts_fr"].map(lambda strs: ' '.join(strs))
metadata.loc[:, "text_en_concat"] = metadata.loc[:, "texts_en"].map(lambda strs: ' '.join(strs))

all_texts = list(set(itertools.chain.from_iterable(metadata.loc[:, "texts"])))

all_texts_embeddings = []

token_count = 0


def json_object(row, sentid, text, vector, model_name):
    authIdHal_s = [] if str(row["authIdHal_s"]) == 'nan' else row["authIdHal_s"].split('§§§')
    authIdHal_i = [] if str(row["authIdHal_i"]) == 'nan' else row["authIdHal_i"].split('§§§')
    authLastNameFirstName_s = row["authLastNameFirstName_s"].split('§§§')
    return {
        'docid': str(row["docid"]),
        'sentid': sentid,
        'text': text,
        'authIdHal_i': str(authIdHal_i),
        'authIdHal_s': authIdHal_s,
        'uuid': str(uuid.uuid1()),
        'authLastNameFirstName_s': authLastNameFirstName_s,
        'model': model_name,
        'vector': list(map(str, list(vector)))}


for index, row in metadata.iterrows():
    texts = row["texts"]
    text_fr_concat = row["text_fr_concat"].strip()
    text_en_concat = row["text_en_concat"].strip()

    if len(texts) == 0:
        continue
    sbert_embeddings = sbert_model.encode(texts)
    fr_text_embedding = None
    if len(text_fr_concat) > 0:
        fr_text_embedding = get_embedding(text_fr_concat)
    en_text_embedding = None
    if len(text_en_concat) > 0:
        en_text_embedding = get_embedding(text_en_concat)
    data_structs = list(
        map(lambda arr: json_object(row, arr[0], arr[1], arr[2], 'sbert'),
            zip(range(0, len(texts)), texts, sbert_embeddings)))
    if en_text_embedding is not None:
        data_structs += [json_object(row, 0, text_en_concat, en_text_embedding, 'ada')]
    if fr_text_embedding is not None:
        data_structs += [json_object(row, 0, text_fr_concat, fr_text_embedding, 'ada')]
    print(f"Index : {index}")
    time.sleep(5)
    print(f"Word count : {sum([len(i.split(' ')) for i in texts])}")
    for data_struct in data_structs:
        json_dump = json.dumps(data_struct, indent=4)

        with open(f"{output_dir}/{str(row['docid'])}-{data_struct['sentid']}-{data_struct['model']}.json",
                  "w") as outfile:
            outfile.write(json_dump)
