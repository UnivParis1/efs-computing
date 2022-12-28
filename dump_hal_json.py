#!/usr/bin/env python
import argparse
import asyncio
import csv
import itertools
import os
from pathlib import Path

import httpx as httpx
import requests

DEFAULT_OUTPUT_DIR_NAME = f"{os.path.expanduser('~')}/hal_dump_json"
DEFAULT_OUTPUT_FILE_NAME = "[DIR]/[DOCID].bib"
DEFAULT_ROWS = 10000

HAL_API_URL = "https://api.archives-ouvertes.fr/search/halshs/?"

LIST_QUERY_TEMPLATE = "q=docType_s:(ART OR OUV OR COUV OR COMM OR THESE OR HDR OR REPORT OR NOTICE OR PROCEEDINGS)" \
                      "&cursorMark=[CURSOR]" \
                      "&sort=docid asc&rows=[ROWS]" \
                      "&fq=instStructAcronym_sci:UP1" \
                      "&fl=docid,fr_title_s,en_title_s,fr_subTitle_s,en_subTitle_s,fr_abstract_s,en_abstract_s,fr_keyword_s,en_keyword_s,structId_i,authIdHal_i,authIdHal_s,authLastNameFirstName_s,docType_s,ePublicationDate_s," \
                      "instStructAcronym_s,labStructAcronym_s,structAcronym_s"

MAX_ATTEMPTS = 10


def parse_arguments():
    parser = argparse.ArgumentParser(description='Fetches HAL SHS bibliographic references in Bibref format.')
    parser.add_argument('--rows', dest='rows',
                        help='Number of requested rows per request', default=DEFAULT_ROWS)
    parser.add_argument('--dir', dest='dir',
                        help='Output directory', default=DEFAULT_OUTPUT_DIR_NAME)
    return parser.parse_args()


def main(args):
    rows = args.rows
    print(f"Rows per request : {rows}")
    directory = args.dir
    Path(directory).mkdir(parents=True, exist_ok=True)
    print(f"Output directory : {directory}")
    cursor = "*"
    total = 0
    output_file = open(f"{directory}/hal_json.csv", 'w')
    print(f"Output to {output_file}")
    csv_writer = csv.writer(output_file)
    first = True
    while True:
        json_request_string = HAL_API_URL + LIST_QUERY_TEMPLATE.replace('[CURSOR]', str(cursor)).replace('[ROWS]',
                                                                                                         str(rows))
        print(f"Request to HAL : {json_request_string}")
        response = requests.get(json_request_string, timeout=360)
        json_response = response.json()
        if not total:
            total = int(json_response['response']['numFound'])
            print(f"{total} entries")
        cursor = json_response['nextCursorMark']
        docs = json_response['response']['docs']
        for doc in docs:
            if first:
                first = False
                csv_writer.writerow(['docid',
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
                                     ])
            csv_writer.writerow([doc.get('docid', ['']),
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
                                 doc.get('ePublicationDate_s', ''),
                                 ])
        if len(docs) == 0:
            print("Download complete !")
            break
    output_file.close()


if __name__ == '__main__':
    main(parse_arguments())
