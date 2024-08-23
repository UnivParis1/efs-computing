import hashlib
import os

import pandas as pd

FIELDS_TO_HASH = ['docid',
                  'fr_title',
                  'en_title',
                  'fr_subtitle',
                  'en_subtitle',
                  'fr_abstract',
                  'en_abstract',
                  'fr_keyword',
                  'en_keyword',
                  'authors',
                  'doc_type',
                  'publication_date',
                  'citation_ref',
                  'citation_full'
                  ]


def create_update_hash(data):
    # Function to create/update hash from external sources
    # Use the FIELDS_TO_HASH variable to determine which fields to hash

    string_to_hash = ''.join(str(data[field]).lower() for field in FIELDS_TO_HASH)

    # Hash the string using hashlib.sha256()
    hashed_value = hashlib.sha256(string_to_hash.encode("utf-8")).hexdigest()

    return hashed_value


def update_hash_on_csv(csv_path, chunksize=10000):
    chunks = []
    for chunk in pd.read_csv(csv_path, chunksize=chunksize):
        chunk['hash'] = chunk.apply(create_update_hash, axis=1)
        chunks.append(chunk)
    updated_data = pd.concat(chunks, ignore_index=True)
    updated_data.sort_values(by=['docid'], inplace=True)
    updated_data.to_csv(csv_path, index=False, mode='w')


if __name__ == '__main__':
    directory = f"{os.path.expanduser('~')}/hal_dump"
    file = "dump.csv"
    file_path = f"{directory}/{file}"
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_path} not found")
    update_hash_on_csv(file_path)
