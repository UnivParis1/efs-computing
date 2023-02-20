#!/usr/bin/env python
import argparse
import hashlib
import logging
import os
from pathlib import Path

import pandas as pd

from hal_api_client import HalApiClient
from log_handler import LogHandler
from mail_sender import MailSender

COLUMNS = ['docid',
           'fr_title',
           'en_title',
           'fr_subtitle',
           'en_subtitle',
           'fr_abstract',
           'en_abstract',
           'fr_keyword',
           'en_keyword',
           'authors',
           'affiliations',
           'doc_type',
           'publication_date',
           'citation_ref',
           'citation_full',
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
    return publications.astype(dtype={"docid": "int32", "created": bool, "updated": bool})


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
    form_ids = doc['authIdForm_i']
    identifiers_mappings = [i.split(HalApiClient.FACET_SEP) for i in doc['authFullNameFormIDPersonIDIDHal_fs']]
    identifiers_dicts = [
        {'name': i[0], 'hal_id': i[1], 'form_id': i[1].split('-')[0], 'idhal_i': i[1].split('-')[1], 'idhal_s': i[2]}
        for i in identifiers_mappings]
    affiliations = [i.split(HalApiClient.FACET_SEP) for i in doc['authIdHasStructure_fs']]
    affiliations_dicts = []
    for affiliation in affiliations:
        identifiers = list(filter(lambda arr: arr['hal_id'] == affiliation[0], identifiers_dicts))
        # dedup
        identifiers = [dict(t) for t in {tuple(d.items()) for d in identifiers}]
        assert len(identifiers) == 1
        org_id = int(affiliation[1].split(HalApiClient.JOIN_SEP)[1])
        org_name = affiliation[2]
        lab = org_id in doc.get('labStructId_i', [])
        affiliations_dicts.append(
            {'hal_id': identifiers[0]['hal_id'], 'org_id': org_id, 'org_name': org_name, 'lab': '1' if lab else '0'})
    values = [int(doc.get('docid')),
              doc.get('fr_title_s', [''])[0],
              doc.get('en_title_s', [''])[0],
              doc.get('fr_subTitle_s', [''])[0],
              doc.get('en_subTitle_s', [''])[0],
              doc.get('fr_abstract_s', [''])[0],
              doc.get('en_abstract_s', [''])[0],
              "§§§".join(doc.get('fr_keyword_s', [''])),
              "§§§".join(doc.get('en_keyword_s', [''])),
              str(identifiers_dicts),
              str(affiliations_dicts),
              doc.get('docType_s', ''),
              doc.get('publicationDate_tdate', ''),
              doc.get('citationRef_s', ''),
              doc.get('citationFull_s', '')
              ]
    return values


def parse_arguments():
    parser = argparse.ArgumentParser(description='Fetches HAL bibliographic references in CSV format.')
    parser.add_argument('--days', dest='days',
                        help='Number of last days modified ou published item to fetch from Hal', default=None,
                        required=False, type=int)
    parser.add_argument('--rows', dest='rows',
                        help='Number of requested rows per request', default=DEFAULT_ROWS, required=False, type=int)
    parser.add_argument('--dir', dest='dir',
                        help='Output directory', required=False, default=DEFAULT_OUTPUT_DIR_NAME)
    parser.add_argument('--file', dest='file',
                        help='Output CSV file name', required=False, default=DEFAULT_OUTPUT_FILE_NAME)
    parser.add_argument('--filter_documents', dest='filter_documents',
                        help='Limited set of document types', required=False, default=False, type=bool)
    return parser.parse_args()


def main(args):
    global logger
    logger = LogHandler('dump_hal_csv', 'log', 'dump_hal_csv.log', logging.DEBUG).create_rotating_log()
    days = args.days
    if days is None:
        message0 = "Missing days parameters : fetch the whole HAL database"
        logger.info(message0)
    else:
        message0 = f"Fetched the last {days} days modified or added publications from HAL database"
        logger.info(message0)
    filter_documents = args.filter_documents
    if filter_documents is False:
        logger.info("All type of documents requested")
    else:
        logger.info(f"Limited set of doctypes")
    rows = args.rows
    logger.info(f"Rows per request : {rows}")
    directory = args.dir
    if not os.path.exists(directory):
        Path(directory).mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory {directory}")
    file = args.file
    file_path = f"{directory}/{file}"
    logger.info(f"Output path : {file_path}")
    publications = load_or_create_publications_df(file_path)
    hal_api_client = HalApiClient(days=days, rows=rows, logger=logger, filtered=filter_documents)
    created, updated, unchanged = 0, 0, 0
    while True:
        cursor, docs = hal_api_client.fetch_publications()
        new_lines = []
        for doc in docs:
            docid = int(doc.get('docid'))
            assert docid is not None
            selection = publications.loc[publications['docid'] == docid]
            existing_hash = None
            if len(selection) > 0:
                logger.debug(f"{docid} exists in csv file")
                existing_line = dict(selection.iloc[0])
                existing_hash = existing_line['hash']
            new_values = extract_fields(doc)
            new_values_hash = hashlib.sha256('-'.join(map(str, new_values)).encode("utf-8")).hexdigest()
            new_values.append(str(new_values_hash))
            if existing_hash is not None:
                if new_values_hash != existing_hash:
                    new_values.extend([False, True])
                    publications[publications['docid'] == docid] = new_values
                    logger.debug(f"{docid} updated")
                    updated += 1
                else:
                    unchanged += 1
                    logger.debug(f"{docid} unchanged")
            else:
                new_values.extend([True, False])
                new_lines.append(new_values)
                created += 1
                logger.debug(f"{docid} created")
        if len(docs) == 0:
            logger.info("Download complete !")
            break
        else:
            publications = pd.concat([publications, pd.DataFrame(new_lines, columns=COLUMNS).astype(
                dtype={"created": bool, "updated": bool})])
    publications.to_csv(file_path, index=False)
    message1 = f"Publications file created or updated at {file_path}"
    message2 = f"Unchanged : {unchanged}, Created : {created}, Updated: {updated}"
    logger.info(message1)
    logger.info(message2)
    MailSender().send_email(type=MailSender.INFO, text=message0 + "\n" + message1 + "\n" + message2)


if __name__ == '__main__':
    try:
        main(parse_arguments())
    except Exception as e:
        logger.exception(f"Hal dump failure : {e}")
        MailSender().send_email(type=MailSender.ERROR, text=f"Hal dump failure : {e}\n{traceback.format_exc()}")
