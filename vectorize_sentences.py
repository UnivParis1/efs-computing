import itertools
import json
import os
import time
import uuid

import nltk
import pandas as pd
from sentence_transformers import SentenceTransformer

model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2')
model.to('cuda')

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


metadata["texts"] = metadata["fr_title_s"].map(lambda title: [title] if type(title) == str else []) + \
                    metadata["en_title_s"].map(lambda title: [title] if type(title) == str else []) + \
                    metadata["fr_abstract_s"].map(split_into_sentences) + \
                    metadata["en_abstract_s"].map(split_into_sentences)

all_texts = list(set(itertools.chain.from_iterable(metadata["texts"])))

all_texts_embeddings = []

for index, row in metadata.iterrows():
    print(index)
    texts = row["texts"]
    authIdHal_s = [] if str(row["authIdHal_s"]) == 'nan' else row["authIdHal_s"].split('§§§')
    authIdHal_i = [] if str(row["authIdHal_i"]) == 'nan' else row["authIdHal_i"].split('§§§')
    authLastNameFirstName_s = row["authLastNameFirstName_s"].split('§§§')
    if len(texts) == 0:
        continue
    embeddings = model.encode(texts)
    hashes = list(
        map(lambda arr: {
            'docid': str(row["docid"]),
            'sentid': arr[0],
            'text': arr[1],
            'authIdHal_i': str(authIdHal_i),
            'authIdHal_s': authIdHal_s,
            'uuid': str(uuid.uuid1()),
            'authLastNameFirstName_s': authLastNameFirstName_s,
            'vector': list(map(str, list(arr[2])))},
            zip(range(0, len(texts)), texts, embeddings)))
    for hash in hashes:
        json_object = json.dumps(hash, indent=4)

        with open(f"{output_dir}/{str(row['docid'])}-{hash['sentid']}.json",
                  "w") as outfile:
            outfile.write(json_object)
