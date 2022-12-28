import itertools

import nltk
import numpy
import pandas as pd
from nltk.corpus import stopwords

nltk.download('punkt')
nltk.download('stopwords')

csv = pd.read_csv(f"{os.path.expanduser('~')}/hal_dump_json/hal_json.csv")
fr_metadata = csv[['docid', 'fr_title_s', 'fr_abstract_s', 'authIdHal_s', 'authIdHal_i', 'authLastNameFirstName_s']]


def split_into_sentences(text):
    if type(text) == str:
        return nltk.sent_tokenize(text)
    return []


fr_metadata["fr_texts"] = fr_metadata["fr_title_s"].map(lambda title: [title] if type(title) == str else []) + \
                          fr_metadata["fr_abstract_s"].map(split_into_sentences)
fr_texts = list(set(itertools.chain.from_iterable(fr_metadata["fr_texts"])))
vectorizer = TfidfVectorizer(stop_words=stopwords.words('french'))
X = vectorizer.fit_transform(fr_texts)
vectorizer.get_feature_names_out()

for index, row in fr_metadata.iterrows():
    print(index)
    weights = map(lambda text: numpy.average(vectorizer.transform([text]).data), texts)
