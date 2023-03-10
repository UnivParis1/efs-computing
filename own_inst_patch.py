import argparse
import ast
import logging
import os

import pandas as pd
import weaviate
from dotenv import dotenv_values

from hal_utils import choose_author_identifier
from log_handler import LogHandler
from uuid_provider import UUIDProvider

OWN_INST_ORG_ID = 7550

DEFAULT_INPUT_DIR_NAME = f"{os.path.expanduser('~')}/hal_dump"
DEFAULT_INPUT_FILE_NAME = "dump.csv"

weaviate_params = dict(dotenv_values(".env.weaviate"))


def get_client():
    return weaviate.Client(weaviate_params['host'], timeout_config=(1000, 1000))


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Fix affiliation errors.')
    parser.add_argument('--csv_dir', dest='csv_dir',
                        help='CSV input file directory', required=False, default=DEFAULT_INPUT_DIR_NAME)
    parser.add_argument('--csv_file', dest='csv_file',
                        help='CSV input file name', required=False, default=DEFAULT_INPUT_FILE_NAME)
    parser.add_argument('--dry', dest='dry',
                        help='Dry run', required=False,
                        default=False, type=bool)
    return parser.parse_args()


def main(args):
    global logger
    dry = args.dry
    if not dry:
        client = get_client()
    logger = LogHandler("own_inst_patch", 'log', 'own_inst_patch.log',
                        logging.INFO).create_rotating_log()
    directory = args.csv_dir
    file = args.csv_file
    file_path = f"{directory}/{file}"
    csv = pd.read_csv(file_path)
    logger.info(f"Total number of documents : {len(csv)}")

    metadata = csv[
        ['authors', 'affiliations']]

    total = len(metadata)
    docs_counter = 0
    authors_data_struct = {}
    for index, row in metadata.iterrows():
        docs_counter += 1
        try:
            affiliations = ast.literal_eval(row['affiliations'])
            authors_data = ast.literal_eval(row['authors'])
        except SyntaxError as e:
            logger.debug(e)
            print(e)
            continue

        for auth in authors_data:
            if auth['hal_id'] not in authors_data_struct:
                identifier = choose_author_identifier(auth)
                authors_data_struct[auth['hal_id']] = auth | {'own_inst': False,
                                                              'identifier': identifier,
                                                              'uuid': str(
                                                                  UUIDProvider(f"hal-auth-{identifier}").value())}
        for affiliation in affiliations:
            org_id = affiliation['org_id']
            hal_id = affiliation['hal_id']
            authors_data_struct[hal_id]['own_inst'] |= (org_id == OWN_INST_ORG_ID)

        logger.info(f"Index : {index} Count : {docs_counter}/{total}")
    for auth in authors_data_struct.values():
        author_uuid = auth['uuid']
        author_name = auth['name']
        author_own_inst = auth['own_inst']
        logger.info(f"{author_name} ({author_uuid}) : {author_own_inst}")
        if author_own_inst is True:
            logger.info(f"Updating own_inst for {author_name}")
        else:
            continue
        if not dry:
            if client.data_object.exists(class_name='Author',
                                         uuid=author_uuid
                                         ):
                client.data_object.update(
                    data_object={"own_inst": True},
                    class_name='Author',
                    uuid=author_uuid
                )
            else:
                logger.error(f"Author with UUID {author_uuid} does not exist.")


if __name__ == '__main__':
    main(parse_arguments())
