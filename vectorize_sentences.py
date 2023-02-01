import argparse
import ast
import json
import logging
import os
from pathlib import Path

import nltk
import openai
import pandas as pd
from dotenv import dotenv_values
from sentence_transformers import SentenceTransformer

from hal_utils import choose_author_identifier
from log_handler import LogHandler
from uuid_provider import UUIDProvider

OWN_INST_ORG_ID = 7550

DEFAULT_OUTPUT_DIR_NAME = f"{os.path.expanduser('~')}/hal_embeddings"
DEFAULT_INPUT_DIR_NAME = f"{os.path.expanduser('~')}/hal_dump"
DEFAULT_INPUT_FILE_NAME = "dump.csv"

EMBEDDING_MODEL = 'text-embedding-ada-002'
EMBEDDING_CTX_LENGTH = 8191
EMBEDDING_ENCODING = 'cl100k_base'

MIN_SENTENCE_LENGTH = 20

sbert_model = SentenceTransformer('sentence-transformers/paraphrase-multilingual-mpnet-base-v2')

nltk.download('punkt')


def sent_json_object(row, pub_uuid, sentid, text, vector, model_name):
    docid = str(row["docid"])
    return {
        'pub_uuid': pub_uuid,
        'docid': docid,
        'sentid': sentid,
        'text': text,
        'uuid': str(UUIDProvider(f"hal-sent-{docid}-{sentid}").value()),
        'model': model_name,
        'vector': list(map(str, list(vector)))}


def split_into_sentences(text):
    if type(text) == str:
        return nltk.sent_tokenize(text)
    return []


def get_openai_embedding(text_or_tokens, model=EMBEDDING_MODEL):
    return openai.Embedding.create(input=text_or_tokens, model=model)["data"][0]["embedding"]


def parse_arguments():
    parser = argparse.ArgumentParser(description='Converts HAL bibliographic references to embeddings for hal import.')
    parser.add_argument('--csv_dir', dest='csv_dir',
                        help='CSV input file directory', required=False, default=DEFAULT_INPUT_DIR_NAME)
    parser.add_argument('--output_dir', dest='output_dir',
                        help='Output directory', required=False, default=DEFAULT_OUTPUT_DIR_NAME)
    parser.add_argument('--csv_file', dest='csv_file',
                        help='CSV input file name', required=False, default=DEFAULT_INPUT_FILE_NAME)
    parser.add_argument('--openai', dest='openai',
                        help='Enable openai embeddings', required=False, default=False, type=bool)
    return parser.parse_args()


def enable_openai():
    openai_params = dict(dotenv_values(".env.openai"))
    openai.organization = openai_params['organization']
    openai.api_key = openai_params['api_key']


def main(args):
    global logger
    logger = LogHandler("vectorize_sentences", 'log', 'vectorize_sentences.log', logging.INFO).create_rotating_log()
    openai = args.openai
    if openai:
        enable_openai()
    logger.info("OpenAI embdeddings " + ("enabled" if openai else "disabled"))
    directory = args.csv_dir
    file = args.csv_file
    file_path = f"{directory}/{file}"
    csv = pd.read_csv(file_path)
    copy = csv.copy()
    copy = copy.query('updated!=0 | created!=0')

    metadata = copy[
        ['docid', 'fr_title', 'en_title', 'fr_subtitle', 'en_subtitle', 'fr_abstract', 'en_abstract', 'fr_keyword',
         'en_keyword', 'authors', 'affiliations', 'doc_type', 'publication_date', 'citation_ref', 'citation_full']]
    output_dir = args.output_dir
    Path(output_dir).mkdir(parents=True, exist_ok=True)

    metadata.loc[:, "texts_en"] = copy.loc[:, "en_title"].map(
        lambda title: [title] if type(title) == str else []) + \
                                  copy.loc[:, "en_abstract"].map(split_into_sentences)
    metadata.loc[:, "texts_fr"] = copy.loc[:, "fr_title"].map(
        lambda title: [title] if type(title) == str else []) + \
                                  copy.loc[:, "fr_abstract"].map(split_into_sentences)
    metadata.loc[:, "texts"] = metadata.loc[:, "texts_fr"] + metadata.loc[:, "texts_en"]

    metadata.loc[:, "text_fr_concat"] = metadata.loc[:, "texts_fr"].map(lambda strs: ' '.join(strs))
    metadata.loc[:, "text_en_concat"] = metadata.loc[:, "texts_en"].map(lambda strs: ' '.join(strs))
    total = len(metadata)
    for index, row in metadata.iterrows():
        affiliations = ast.literal_eval(row['affiliations'])
        lab_data_struct = {}
        inst_data_struct = {}
        pub_data_struct = {}
        authors_data_struct = {auth['hal_id']: auth | {'has_lab': [], 'has_inst': [], 'own_inst': False} for auth in
                               ast.literal_eval(row['authors'])}
        for key in authors_data_struct:
            identifier = choose_author_identifier(authors_data_struct[key])
            authors_data_struct[key]['uuid'] = str(UUIDProvider(f"hal-auth-{identifier}").value())
            authors_data_struct[key]['identifier'] = identifier
        pub_uuid = str(UUIDProvider(f"hal-pub-{row['docid']}").value())
        pub_data_struct[row['docid']] = {
                                            'uuid': pub_uuid,
                                            'docid': row['docid'],
                                            'fr_title': row['fr_title'],
                                            'en_title': row['en_title'],
                                            'fr_subtitle': row['fr_subtitle'],
                                            'en_subtitle': row['en_subtitle'],
                                            'fr_abstract': row['fr_abstract'],
                                            'en_abstract': row['en_abstract'],
                                            'fr_keyword': row['fr_keyword'],
                                            'en_keyword': row['en_keyword'],
                                            'doc_type': row['doc_type'],
                                            'publication_date': row['publication_date'],
                                            'citation_ref': row['citation_ref'],
                                            'citation_full': row['citation_full'],
                                            'text_fr_concat': row['text_fr_concat'],
                                            'text_en_concat': row['text_en_concat'],
                                            'has_authors': list(
                                                map(lambda auth: auth['uuid'], authors_data_struct.values()))
                                        } | {'has_lab': [], 'has_inst': []}
        buffer = []
        for affiliation in affiliations:
            is_lab = int(affiliation['lab']) == 1
            prop_name = 'has_lab' if is_lab else 'has_inst'
            org_id = affiliation['org_id']
            org_uuid = str(UUIDProvider(f"hal-org-{org_id}").value())
            org = {'id': org_id, 'name': affiliation['org_name'], 'uuid': org_uuid, 'lab': str(1 if is_lab else 0)}
            if is_lab:
                lab_data_struct[org_id] = org
            else:
                inst_data_struct[org_id] = org
            if org_id in buffer:
                continue
            buffer.append(org_id)
            pub_data_struct[row['docid']][prop_name].append(org_uuid)
            authors_data_struct[affiliation['hal_id']][prop_name].append(org_uuid)
            authors_data_struct[affiliation['hal_id']]['own_inst'] |= (org_id == OWN_INST_ORG_ID)
        texts = row["texts"]
        text_fr_concat = row["text_fr_concat"].strip()
        text_en_concat = row["text_en_concat"].strip()
        texts = list(filter(lambda text: len(text) > MIN_SENTENCE_LENGTH, texts))
        if len(texts) == 0:
            continue
        sbert_embeddings = sbert_model.encode(texts)
        ada_embeddings = [get_openai_embedding(text) for text in texts] if openai else []
        fr_text_embedding = None
        if openai and len(text_fr_concat) > 0:
            fr_text_embedding = get_openai_embedding(text_fr_concat)
        en_text_embedding = None
        if openai and len(text_en_concat) > 0:
            en_text_embedding = get_openai_embedding(text_en_concat)
        fr_title_embeddings = None
        if not pd.isna(row['fr_title']):
            fr_title_embeddings = sbert_model.encode(row['fr_title'])
        en_title_embeddings = None
        if not pd.isna(row['en_title']):
            en_title_embeddings = sbert_model.encode(row['en_title'])
        sent_data_structs = list(
            map(lambda arr: sent_json_object(row, pub_uuid, arr[0], arr[1], arr[2], 'sbert'),
                zip(range(0, len(texts)), texts, sbert_embeddings))) + list(
            map(lambda arr: sent_json_object(row, pub_uuid, arr[0], arr[1], arr[2], 'ada'),
                zip(range(0, len(texts)), texts, ada_embeddings)))
        if en_text_embedding is not None:
            pub_data_struct[row['docid']]['text_ada_en_embed'] = list(map(str, list(en_text_embedding)))
        if fr_text_embedding is not None:
            pub_data_struct[row['docid']]['text_ada_fr_embed'] = list(map(str, list(fr_text_embedding)))
        if en_title_embeddings is not None:
            pub_data_struct[row['docid']]['title_sbert_en_embed'] = list(map(str, list(en_title_embeddings)))
        if fr_title_embeddings is not None:
            pub_data_struct[row['docid']]['title_sbert_fr_embed'] = list(map(str, list(fr_title_embeddings)))
        logger.info(f"Count : {index}/{total}")
        logger.debug(f"Word count : {sum([len(i.split(' ')) for i in texts])}")
        dump_to_json('sent', sent_data_structs, output_dir, suffix='model')
        dump_to_json('lab', lab_data_struct.values(), output_dir)
        dump_to_json('inst', inst_data_struct.values(), output_dir)
        dump_to_json('auth', authors_data_struct.values(), output_dir)
        dump_to_json('pub', pub_data_struct.values(), output_dir)
        csv.loc[csv['docid'] == row['docid'], 'created'] = False
        csv.loc[csv['docid'] == row['docid'], 'updated'] = False
        if index % 1000 == 0:
            logger.debug(f"Saving csv at index {index}")
            csv.to_csv(file_path, index=False)
    csv.loc[:, 'created'] = False
    csv.loc[:, 'updated'] = False
    csv.to_csv(file_path, index=False)


def dump_to_json(prefix, data, output_dir, suffix=None):
    for data_struct in data:
        json_dump = json.dumps(data_struct, indent=4)
        with open(
                f"{output_dir}/{prefix}-{str(data_struct['uuid'])}{('-' + data_struct[suffix]) if suffix else ''}.json",
                "w") as outfile:
            outfile.write(json_dump)


if __name__ == '__main__':
    main(parse_arguments())
