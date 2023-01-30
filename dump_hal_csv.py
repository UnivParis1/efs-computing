#!/usr/bin/env python
import argparse
import hashlib
import os
from pathlib import Path
from random import random

import pandas as pd

from hal_api_client import HalApiClient

COLUMNS = ['docid',
           'fr_title_s',
           'en_title_s',
           'fr_subTitle_s',
           'en_subTitle_s',
           'fr_abstract_s',
           'en_abstract_s',
           'fr_keyword_s',
           'en_keyword_s',
           'authIdHal_i',
           'authIdHal_s',
           'authLastNameFirstName_s',
           'labStructAcronym_s',
           'instStructAcronym_s',
           'structAcronym_s',
           'docType_s',
           'ePublicationDate_s',
           'hash',
           'created',
           'updated'
           ]

DEFAULT_OUTPUT_DIR_NAME = f"{os.path.expanduser('~')}/hal_dump"
DEFAULT_OUTPUT_FILE_NAME = "dump.csv"
DEFAULT_ROWS = 10000


def load_or_create_publications_df(file_path: str) -> pd.DataFrame:
    """Load publications DataFrame from CSV if exists, or create it

    Parameters
    ----------
    file_path : str, required
            CSV file path

    Returns
    -------
    publications: pd.DataFrame Publications table
    """
    if not os.path.exists(file_path):
        publications = pd.DataFrame(columns=COLUMNS)
        # publications = publications.set_index('docid')
    else:
        # publications = pd.read_csv(file_path, header=0, index_col='docid')
        publications = pd.read_csv(file_path, header=0)
    return publications.astype(dtype={"docid": "int32"})


def extract_fields(doc: dict) -> list:
    """Extracts useful fields from HAL json response and formats for csv


    Parameters
    ----------
    doc : dict, required
            json response from Hal

    Returns
    -------
    values: list of formatted values
    """
    values = [int(doc.get('docid')),
              doc.get('fr_title_s', [''])[0],
              doc.get('en_title_s', [''])[0],
              doc.get('fr_subTitle_s', [''])[0],
              doc.get('en_subTitle_s', [''])[0],
              doc.get('fr_abstract_s', [''])[0],
              doc.get('en_abstract_s', [''])[0],
              "§§§".join(doc.get('fr_keyword_s', [''])),
              "§§§".join(doc.get('en_keyword_s', [''])),
              "§§§".join(map(str, doc.get('authIdHal_i', ['']))),
              "§§§".join(doc.get('authIdHal_s', [''])),
              "§§§".join(doc.get('authLastNameFirstName_s', [''])),
              "§§§".join(doc.get('labStructAcronym_s', [''])),
              "§§§".join(doc.get('instStructAcronym_s', [''])),
              "§§§".join(doc.get('structAcronym_s', [''])),
              doc.get('docType_s', ''),
              doc.get('ePublicationDate_s', '')
              ]
    return values


def parse_arguments():
    parser = argparse.ArgumentParser(description='Fetches HAL SHS bibliographic references in Bibref format.')
    parser.add_argument('--days', dest='days',
                        help='Number of last days modified ou published item to fetch from Hal', default=None,
                        required=False, type=int)
    parser.add_argument('--rows', dest='rows',
                        help='Number of requested rows per request', default=DEFAULT_ROWS, required=False, type=int)
    parser.add_argument('--dir', dest='dir',
                        help='Output directory', required=False, default=DEFAULT_OUTPUT_DIR_NAME)
    parser.add_argument('--file', dest='file',
                        help='Output CSV file name', required=False, default=DEFAULT_OUTPUT_FILE_NAME)
    return parser.parse_args()


def main(args):
    days = args.days
    if days is None:
        print("Missing days parameters : fetch the whole HAL database")
    else:
        print(f"Fetch the last {days} days modified or added publications from HAL database")
    rows = args.rows
    print(f"Rows per request : {rows}")
    directory = args.dir
    if not os.path.exists(directory):
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"Created directory {directory}")
    file = args.file
    file_path = f"{directory}/{file}"
    print(f"Output path : {file_path}")
    publications = load_or_create_publications_df(file_path)
    hal_api_client = HalApiClient(days=days, rows=rows, verbose=True)
    while True:
        cursor, docs = hal_api_client.fetch_publications()
        new_lines = []
        for doc in docs:
            docid = int(doc.get('docid'))
            assert docid is not None
            selection = publications.loc[publications['docid'] == docid]
            existing_hash = None
            if len(selection) > 0:
                existing_line = dict(selection.iloc[0])
                existing_hash = existing_line['hash']
            new_values = extract_fields(doc)
            new_values_hash = hashlib.sha256('-'.join(map(str, new_values)).encode("utf-8")).hexdigest()
            new_values.append(str(new_values_hash))
            if existing_hash is not None:
                if new_values_hash != existing_hash:
                    new_values.extend([False, True])
                    publications[publications['docid'] == docid] = new_values
            else:
                new_values.extend([True, False])
                new_lines.append(new_values)
        if len(docs) == 0:
            print("Download complete !")
            break
        else:
            publications = pd.concat([publications, pd.DataFrame(new_lines, columns=COLUMNS)])
    publications.to_csv(file_path, index=False)
    print(f"Publications file created or updated at {file_path}")


if __name__ == '__main__':
    main(parse_arguments())
