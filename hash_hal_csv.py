import os

import pandas as pd

from data_hash import compute_hash


def update_hash_on_csv(csv_path, chunksize=10000):
    chunks = []
    for chunk in pd.read_csv(csv_path, chunksize=chunksize):
        chunk['hash'] = chunk.apply(compute_hash, axis=1)
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
