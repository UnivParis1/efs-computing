import argparse
import glob
import json
import logging
import os
import uuid
from pathlib import Path

import numpy as np
import weaviate

from hal_utils import choose_author_identifier
from log_handler import LogHandler

KEYWORDS_SEPARATOR = '§§§'

DEFAULT_INPUT_DIR_NAME = f"{os.path.expanduser('~')}/hal_embeddings"

sentence_class = {
    "class": "Sentence",
    "description": "Isolated sentence",
    "properties": [
        {
            "dataType": [
                "text"
            ],
            "description": "The text of the sentence",
            "name": "text",
        }, {
            "dataType": [
                "string"
            ],
            "description": "Model : 'sbert' or 'ada",
            "name": "model",
        },
        {
            "dataType": [
                "string"
            ],
            "description": "docid",
            "name": "docid"
        },
        {
            "dataType": [
                "int"
            ],
            "description": "Sentence id",
            "name": "sentid"
        }
    ]
}

publication_class = {
    "class": "Publication",
    "description": "Publications of all kind",
    "properties": [
        {
            "dataType": [
                "text"
            ],
            "description": "The french title of the sentence",
            "name": "fr_title",
        },
        {
            "dataType": [
                "text"
            ],
            "description": "The english title of the sentence",
            "name": "en_title",
        },
        {
            "dataType": [
                "text"
            ],
            "description": "The french subtitle of the sentence",
            "name": "fr_subtitle",
        },
        {
            "dataType": [
                "text"
            ],
            "description": "The english subtitle of the sentence",
            "name": "en_subtitle",
        }, {
            "dataType": [
                "text"
            ],
            "description": "The french abstract of the sentence",
            "name": "fr_abstract",
        },
        {
            "dataType": [
                "text"
            ],
            "description": "The english abstract of the sentence",
            "name": "en_abstract",
        },
        {
            "dataType": [
                "text"
            ],
            "description": "Hal document type code",
            "name": "doc_type",
        },
        {
            "dataType": [
                "text"
            ],
            "description": "Short citation",
            "name": "citation_ref",
        },
        {
            "dataType": [
                "text"
            ],
            "description": "Long citation",
            "name": "citation_full",
        }, {
            "dataType": [
                "string[]"
            ],
            "description": "French keywords",
            "name": "fr_keyword",
        }, {
            "dataType": [
                "string[]"
            ],
            "description": "English keywords",
            "name": "en_keyword",
        },
        {
            "dataType": [
                "int"
            ],
            "description": "Hal docid",
            "name": "docid"
        }
    ]
}
author_class = {
    "class": "Author",
    "description": "Hal authors",
    "properties": [
        {
            "dataType": [
                "string"
            ],
            "description": "The identifier of the Author, either i or s",
            "name": "identifier",
        },
        {
            "dataType": [
                "string"
            ],
            "description": "The full name of the Author",
            "name": "name",
        },
        {
            "dataType": [
                "boolean"
            ],
            "description": "If the author belongs to our institution",
            "name": "own_inst",
        }
    ]
}

organisation_class = {
    "class": "Organisation",
    "description": "Structures and institutions",
    "properties": [
        {
            "dataType": [
                "string"
            ],
            "description": "The identifier of the organisation",
            "name": "identifier",
        },
        {
            "dataType": [
                "string"
            ],
            "description": "The full name of the organisation",
            "name": "name",
        },
        {
            "dataType": [
                "string"
            ],
            "description": "The type of the organisation, 'lab' or 'inst'",
            "name": "type",
        }
    ]
}

publication_prop = {
    "dataType": [
        "Publication"
    ],
    "name": "hasPublication"
}
author_prop = {
    "dataType": [
        "Author"
    ],
    "name": "hasAuthors"
}
organisation_prop = {
    "dataType": [
        "Organisation"
    ],
    "name": "hasOrganisations"
}


def get_client():
    return weaviate.Client("http://localhost:8080")


def configure(client):
    client.batch.configure(
        batch_size=1000,
        dynamic=True,
        timeout_retries=3,
        callback=None,
    )


def reset(client):
    client.schema.delete_all()
    client.schema.create_class(sentence_class)
    client.schema.create_class(author_class)
    client.schema.create_class(organisation_class)
    client.schema.create_class(publication_class)
    client.schema.property.create("Sentence", publication_prop)
    client.schema.property.create("Publication", author_prop)
    client.schema.property.create("Publication", organisation_prop)
    client.schema.property.create("Author", organisation_prop)


def parse_arguments():
    parser = argparse.ArgumentParser(
        description='Loads HAL bibliographic references, authors and structures to vector database.')
    parser.add_argument('--input_dir', dest='input_dir',
                        help='Json input files directory', required=False, default=DEFAULT_INPUT_DIR_NAME)
    parser.add_argument('--reset', dest='reset',
                        help='Reset database', required=False, default=False, type=bool)
    return parser.parse_args()


def load_org_data(orgs, client, reset_db=False):
    for org in orgs:
        logger.info(f"importing organisation: {org['name']}")
        org_properties = {
            "name": org["name"],
            "identifier": str(org["id"]),
            "type": 'lab' if int(org['lab']) else 'inst',
        }
        clean_properties(org_properties)
        client.batch.add_data_object(org_properties, "Organisation", uuid.UUID(org["uuid"]))
    client.batch.flush()


def load_authors_data(authors, client, reset_db=False):
    for author in authors:
        logger.info(f"Importing author: {author['name']}")
        identifier = choose_author_identifier(author)
        author_uuid = uuid.UUID(author["uuid"])
        author_properties = {
            "name": author["name"],
            "own_inst": author["own_inst"] is True,
            "identifier": str(identifier),
        }
        clean_properties(author_properties)
        client.batch.add_data_object(author_properties, "Author", author_uuid)
        if reset_db:
            for org in author.get('has_lab', []):
                client.batch.add_reference(author["uuid"], 'Author', 'hasOrganisations', org, 'Organisation')
            for org in author.get('has_inst', []):
                client.batch.add_reference(author["uuid"], 'Author', 'hasOrganisations', org, 'Organisation')
    client.batch.flush()


def update_authors_relations(authors, client, reset_db=False):
    assert reset_db is False
    for author in authors:
        logger.info(f"Updating author's relations : {author['name']}")
        client.data_object.reference.update(
            from_uuid=author["uuid"],
            from_property_name='hasOrganisations',
            to_uuids=author.get('has_lab', []),
            from_class_name='Author',
            to_class_names='Organisation',
        )
        client.data_object.reference.update(
            from_uuid=author["uuid"],
            from_property_name='hasOrganisations',
            to_uuids=author.get('has_inst', []),
            from_class_name='Author',
            to_class_names='Organisation',
        )


def load_publication_data(publications, client, reset_db=False):
    for publication in publications:
        logger.info(f"Importing publication: {str(publication['docid'])}")
        publication_uuid = uuid.UUID(publication["uuid"])
        publication_properties = {key: publication[key] for key in publication.keys()
                                  & {'docid', 'fr_title', 'en_title', 'fr_subtitle', 'en_subtitle', 'fr_abstract',
                                     'en_abstract', 'fr_keyword', 'en_keyword', 'doc_type',
                                     'citation_ref', 'citation_full', 'publication_date'}}
        split_keywords(publication_properties, 'fr_keyword')
        split_keywords(publication_properties, 'en_keyword')
        vector = publication.get("text_ada_en_embed", None) or publication.get("text_ada_fr_embed",
                                                                               None) or publication.get(
            "title_sbert_en_embed", None) or publication.get("title_sbert_fr_embed", None)
        clean_properties(publication_properties)
        if vector is not None:
            vector = list(map(float, vector))
        client.batch.add_data_object(publication_properties, "Publication", publication_uuid, vector)
        if reset_db:
            for org in publication.get('has_lab', []):
                client.batch.add_reference(publication["uuid"], 'Publication', 'hasOrganisations', org,
                                           'Organisation')
            for org in publication.get('has_inst', []):
                client.batch.add_reference(publication["uuid"], 'Publication', 'hasOrganisations', org,
                                           'Organisation')
            for auth in publication.get('has_authors', []):
                client.batch.add_reference(publication["uuid"], 'Publication', 'hasAuthors', auth,
                                           'Author')
    client.batch.flush()


def update_publication_relations(publications, client, reset_db=False):
    assert reset_db is False
    for publication in publications:
        logger.info(f"Updating publication's relations : {str(publication['docid'])}")
        client.data_object.reference.update(
            from_uuid=publication["uuid"],
            from_property_name='hasOrganisations',
            to_uuids=publication.get('has_lab', []),
            from_class_name='Publication',
            to_class_names='Organisation',
        )
        client.data_object.reference.update(
            from_uuid=publication["uuid"],
            from_property_name='hasOrganisations',
            to_uuids=publication.get('has_inst', []),
            from_class_name='Publication',
            to_class_names='Organisation',
        )
        client.data_object.reference.update(
            from_uuid=publication["uuid"],
            from_property_name='hasAuthors',
            to_uuids=publication.get('has_authors', []),
            from_class_name='Publication',
            to_class_names='Author',
        )


def load_sent_data(sentences, client, reset_db=False):
    for sentence in sentences:
        logger.info(f"Importing sentence: {sentence['text']}")
        sentence_uuid = uuid.UUID(sentence["uuid"])
        sentence_properties = {
            "model": sentence["model"],
            "docid": sentence["docid"],
            "sentid": int(sentence["sentid"]),
            "text": sentence["text"],
        }
        clean_properties(sentence_properties)
        client.batch.add_data_object(sentence_properties, "Sentence", sentence_uuid,
                                     list(map(float, sentence["vector"])))
        if reset_db:
            client.batch.add_reference(sentence["uuid"], 'Sentence', 'hasPublication', sentence['pub_uuid'],
                                       'Publication')
    client.batch.flush()


def update_sentence_relations(sentences, client, reset_db=False):
    assert reset_db is False
    for sentence in sentences:
        logger.info(f"Updating sentences relations: {sentence['text']}")
        pub_uuid = sentence.get('pub_uuid', None)
        assert pub_uuid is not None
        client.data_object.reference.update(
            from_uuid=sentence["uuid"],
            from_property_name='hasPublication',
            to_uuids=[pub_uuid],
            from_class_name='Sentence',
            to_class_names='Publication',
        )


def split_keywords(publication_properties, keywords_key):
    keywords = publication_properties.get(keywords_key, '')
    keywords = keywords if isinstance(keywords, str) else ''
    publication_properties[keywords_key] = keywords.split(KEYWORDS_SEPARATOR)


def clean_properties(properties):
    for key in properties:
        if type(properties[key]) == float and np.isnan(properties[key]):
            properties[key] = None


def move_files(processed_files):
    for processed_file in processed_files:
        Path(processed_file).rename(f"{Path(processed_file).parent}/processed/{Path(processed_file).name}")


def load_data_from_file_system(client, file_prefix, loading_function, input_dir, processed_files, reset_db=False):
    counter = 0
    data = []
    for f in glob.glob(f"{input_dir}/{file_prefix}*.json"):
        with open(f, ) as infile:
            logger.debug(f"Loading {str(infile.name)} ({counter})")
            data.append(json.load(infile))
            counter = counter + 1
        if counter % 10000 == 0:
            loading_function(data, client, reset_db)
            data = []
        processed_files.add(f)
    loading_function(data, client, reset_db)


def main(args):
    global logger
    logger = LogHandler("weaviate_import", 'log', 'weaviate_import.log', logging.INFO).create_rotating_log()
    client = get_client()
    if args.reset == True:
        logger.info("Resetting weaviate database")
        reset(client)
    input_dir = args.input_dir
    processed_dir = f"{input_dir}/processed"
    Path(processed_dir).mkdir(parents=True, exist_ok=True)
    processed_files = set()
    load_data_from_file_system(client, 'inst', load_org_data, input_dir, processed_files, args.reset)
    move_files(processed_files)
    processed_files = set()
    load_data_from_file_system(client, 'lab', load_org_data, input_dir, processed_files, args.reset)
    move_files(processed_files)
    processed_files = set()
    load_data_from_file_system(client, 'auth', load_authors_data, input_dir, processed_files, args.reset)
    if not args.reset:
        load_data_from_file_system(client, 'auth', update_authors_relations, input_dir, processed_files)
    move_files(processed_files)
    processed_files = set()
    load_data_from_file_system(client, 'pub', load_publication_data, input_dir, processed_files, args.reset)
    if not args.reset:
        load_data_from_file_system(client, 'pub', update_publication_relations, input_dir, processed_files)
    move_files(processed_files)
    processed_files = set()
    load_data_from_file_system(client, 'sent', load_sent_data, input_dir, processed_files, args.reset)
    if not args.reset:
        load_data_from_file_system(client, 'sent', update_sentence_relations, input_dir, processed_files)
    move_files(processed_files)


if __name__ == '__main__':
    main(parse_arguments())
