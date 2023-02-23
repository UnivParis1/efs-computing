import weaviate
from dotenv import dotenv_values


class VectorDatabase:

    @staticmethod
    def build_query(embedding, sentence_class):
        return f"""
        {{
            Get {{
            {sentence_class} (
              limit: 100
              nearVector: {{
                vector: {str(embedding)}
              }}
            ) {{
              docid
              text 
              sentid
              hasPublication {{ 
                ...on Publication {{
                  doc_type
                  docid
                  fr_title
                  en_title
                  fr_abstract
                  en_abstract
                  fr_keyword
                  en_keyword
                  citation_ref
                  citation_full
                  hasAuthors {{
                    ...on Author {{
                      identifier
                      name
                      own_inst
                    }}
                  }}
                }}
              }}
              _additional {{
                distance
                certainty
              }}
            }}
          }}
        }}
        """

    def __init__(self):
        weaviate_params = dict(dotenv_values(".env.weaviate"))
        self.client = weaviate.Client(weaviate_params['host'])

    def results(self, embedding, sentence_class):
        return self.client.query.raw(self.build_query(embedding, sentence_class))
