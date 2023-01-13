import glob
import json
import os
import uuid

import weaviate

MIN_SENTENCE_LENGTH = 20

client = weaviate.Client("http://localhost:8080")

client.schema.delete_all()
sentence_class = {
    "class": "Sentence",  # <= note the capital "A".
    "description": "Isolated sentence",
    "properties": [
        {
            "dataType": [
                "text"
            ],
            "description": "The text of the sentence",
            "name": "text",
        },
        {
            "dataType": [
                "number"
            ],
            "description": "The tf-idf average score",
            "name": "weight"
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
        }
    ]
}

# add the schema
client.schema.create_class(sentence_class)
client.schema.create_class(author_class)

add_prop = {
    "dataType": [
        "Author"
    ],
    "name": "hasAuthors"
}

client.schema.property.create("Sentence", add_prop)

data = []
authors_dict = {}

# Configure a batch process
client.batch.configure(
    batch_size=100,
    dynamic=True,
    timeout_retries=3,
    callback=None,
)


def load_data():
    for sentence in data:
        print("importing sentence: ", sentence["text"])
        if len(sentence["text"]) < MIN_SENTENCE_LENGTH:
            print("Too short, skip")
            continue
        sentence_uuid = uuid.UUID(sentence["uuid"])
        sentence_properties = {
            "docid": sentence["docid"],
            "sentid": int(sentence["sentid"]),
            "text": sentence["text"],
        }
        identifiers = sentence['authIdHal_s'] if len(sentence['authIdHal_s']) == 0 else sentence['authIdHal_s']
        names = sentence['authLastNameFirstName_s']
        client.batch.add_data_object(sentence_properties, "Sentence", sentence_uuid,
                                     list(map(float, sentence["vector"])))
        for index, identifier in enumerate(identifiers):
            if not identifier in authors_dict.keys():
                author_properties = {
                    "identifier": identifier,
                    "name": names[index] if len(names) > index else ''
                }
                author_uuid = uuid.uuid1()
                client.batch.add_data_object(author_properties, "Author", author_uuid)
                authors_dict[identifier] = author_uuid
            else:
                author_uuid = authors_dict[identifier]
            client.batch.add_reference(str(sentence_uuid), 'Sentence', 'hasAuthors', str(author_uuid), 'Author')
    client.batch.flush()


for f in glob.glob(f"{os.path.expanduser('~')}/hal_embeddings/*.json"):
    counter = 0
    with open(f, ) as infile:
        print(f"Loading {str(infile)}...")
        data.append(json.load(infile))
        counter = counter + 1

        if counter % 10000 == 0:
            load_data()
            data = []

load_data()
