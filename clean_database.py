#!/usr/bin/env python
import argparse
import itertools
import logging
import traceback

import weaviate
from dotenv import dotenv_values

from hal_api_client import HalApiClient
from log_handler import LogHandler
from mail_sender import MailSender

DEFAULT_HAL_ROWS = 10000
DEFAULT_WEAVIATE_ROWS = 10000

NUMBER_OF_DOCS_TO_REMOVE_ALERT_LEVEL = 100

weaviate_params = dict(dotenv_values(".env.weaviate"))


def get_client():
    return weaviate.Client(weaviate_params['host'], timeout_config=(1000, 1000))


def parse_arguments():
    parser = argparse.ArgumentParser(description='Fetches HAL bibliographic references in CSV format.')
    parser.add_argument('--hal_rows', dest='hal_rows',
                        help='Number of requested rows per request (HAL)', default=DEFAULT_HAL_ROWS, required=False,
                        type=int)
    parser.add_argument('--weaviate_rows', dest='weaviate_rows',
                        help='Number of requested rows per request (Weaviate)', default=DEFAULT_WEAVIATE_ROWS,
                        required=False, type=int)
    parser.add_argument('--dry', action='store_true', help='Simulate without effective removal')
    parser.add_argument('--force', action='store_true', help='Force removal, overcome limit of number of documents')

    return parser.parse_args()


def main(args):
    global logger
    force = args.force
    dry_run = args.dry
    logger = LogHandler('clean_database', 'log', 'clean_database.log', logging.DEBUG).create_rotating_log()
    hal_rows = args.hal_rows
    weaviate_rows = args.weaviate_rows
    logger.info(f"Rows per request to HAL : {hal_rows}")
    logger.info(f"Rows per request to Weaviate : {weaviate_rows}")
    hal_api_client = HalApiClient(rows=hal_rows, logger=logger)
    it = None
    while True:
        docs = hal_api_client.fetch_all_publication_ids()
        new_it = map(lambda doc: doc['docid'], docs)
        if it is None:
            it = new_it
        else:
            it = itertools.chain(it, new_it)
        if len(docs) == 0:
            logger.info("Download complete !")
            break
    hal_docids = list(map(int, it))

    client = get_client()
    weaviate_docids = {}
    offset = 0
    while True:
        ids = client.query.get("Publication", ["docid"]).with_additional(["id"]).with_limit(weaviate_rows).with_offset(
            offset).do()
        logger.debug(f"Fetch {weaviate_rows} publications with offset {offset}")
        offset += weaviate_rows
        if ids['data']['Get']['Publication'] is None:
            logger.info("All publications fetched from Weaviate !")
            break
        weaviate_docids = weaviate_docids | {elem['docid']: elem['_additional']['id'] for elem in
                                             ids['data']['Get']['Publication']}
    to_remove = list(set(dict.keys(weaviate_docids)) - set(map(int, hal_docids)))
    if len(to_remove) > NUMBER_OF_DOCS_TO_REMOVE_ALERT_LEVEL and not force:
        raise RuntimeError(
            f"abnormal number of documents to remove: {len(to_remove)}, stopping process, check and launch manually")
    removal_list = []
    uuids_to_remove = [weaviate_docids[key] for key in to_remove]
    for uuid in uuids_to_remove:
        citation = client.data_object.get_by_id(uuid=uuid,
                                                class_name='Publication')['properties']['citation_full']
        removal_list.append(
            citation)
        logger.info(f"Remove : {citation}")
        if not dry_run:
            client.data_object.delete(
                uuid=uuid,
                class_name='Publication'
            )

    message = f"Removed {len(to_remove)} from database {'(Simulation)' if dry_run else ''}."
    logger.info(message)
    MailSender().send_email(type=MailSender.INFO, text=message + "\nDetails : \n".join(
        removal_list))


if __name__ == '__main__':
    try:
        main(parse_arguments())
    except Exception as e:
        logger.exception(f"Database cleaning process failure : {e}")
        MailSender().send_email(type=MailSender.ERROR,
                                text=f"Database cleaning process failure : {e}\n{traceback.format_exc()}")
