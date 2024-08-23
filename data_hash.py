import hashlib

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


def compute_hash(data:dict)->str:

    string_to_hash = ''.join(str(data.get(field)).lower() for field in FIELDS_TO_HASH)

    # Hash the string using hashlib.sha256()
    hashed_value = hashlib.sha256(string_to_hash.encode("utf-8")).hexdigest()

    return hashed_value
