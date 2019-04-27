import json
import re
import time

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import Index, Document, Text, Keyword, Integer, Completion
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.analysis import tokenizer, analyzer, token_filter
from elasticsearch_dsl.query import MultiMatch, Match

# Connect to local host server
connections.create_connection(hosts=['127.0.0.1'])

# Create elasticsearch object
es = Elasticsearch()

# Define analyzers appropriate for your data.
# You can create a custom analyzer by choosing among elasticsearch options
# or writing your own functions.
# Elasticsearch also has default analyzers that might be appropriate.
my_analyzer = analyzer('custom1',
                       tokenizer='standard',
                       filter=['lowercase', 'stop'])

# --- Add more analyzers here ---
# use stopwords... or not?
# use stemming... or not?
# the analyzer which tokenize for text, use stopwords stemming, lowercase and ascii-folding
text_analyzer = analyzer('custom2',
                         tokenizer='letter',
                         filter=["stop", "lowercase", "porter_stem", "asciifolding"]
                         )
# the folding analyzer which only use lowercase and ascii-folding
folding_analyzer = analyzer('custom3',
                            tokenizer='standard',
                            filter=["lowercase", "asciifolding"])
# the category analyzer which use lowercase, ascii-folding and stemming
cat_analyzer = analyzer('custom4',
                        tokenizer='standard',
                        filter=["lowercase", "asciifolding", "porter_stem"])


# Define document mapping (schema) by defining a class as a subclass of Document.
# This defines fields and their properties (type and analysis applied).
# You can use existing es analyzers or use ones you define yourself as above.
class Movie(Document):
    title = Text(analyzer=text_analyzer)
    title_suggest = Completion()  # for autocomplete
    text = Text(analyzer=text_analyzer)
    star = Text(analyzer=folding_analyzer)
    runtime = Integer()
    language = Text(analyzer='simple')
    country = Text(analyzer='simple')
    director = Text(analyzer=folding_analyzer)
    location = Text(analyzer='simple')
    time = Text(analyzer='simple')
    categories = Text(analyzer=cat_analyzer)

    # --- Add more fields here ---
    # What data type for your field? List?
    # Which analyzer makes sense for each field?

    # override the Document save method to include subclass field definitions
    def save(self, *args, **kwargs):
        return super(Movie, self).save(*args, **kwargs)


# Populate the index
# when time is not digit, just return 0 instead
def get_stars(stars):
    if stars.isdigit():
        return int(stars)
    else:
        return 0


def buildIndex():
    """
    buildIndex creates a new film index, deleting any existing index of
    the same name.
    It loads a json file containing the movie corpus and does bulk loading
    using a generator function.
    """
    film_index = Index('sample_film_index')
    if film_index.exists():
        film_index.delete()  # Overwrite any previous version
    # create mapping from index to document
    film_index.document(Movie)
    film_index.create()

    # Open the json film corpus
    with open('CAbusinessreview.json', 'r', encoding='utf-8') as data_file:
        # load movies from json file into dictionary
        restaurants = json.load(data_file)
        size = len(restaurants)

    # Action series for bulk loading with helpers.bulk function.
    # Implemented as a generator, to return one movie with each call.
    # Note that we include the index name here.
    # The Document type is always 'doc'.
    # Every item to be indexed must have a unique key.
    def actions():
        # mid is movie id (used as key into movies dictionary)
        for mid in range(1, size + 1):
            yield {
                "_index": "food_finder_index",
                "_type": 'doc',
                "_id": mid,
                "name": restaurants[mid][1],
                "name_suggest": restaurants[mid][1],
                "review": restaurants[mid][11],
                "address": restaurants[mid][2],
                "city": restaurants[mid][5],  # You would like to convert runtime to
                # integer (in minutes) --- Add more fields here ---
                "stars": restaurants[mid][10],
                "country": ', '.join(movies[str(mid)]['Country']),
                "director": ', '.join(movies[str(mid)]['Director']),
                "location": ', '.join(movies[str(mid)]['Location']),
                "time": ', '.join(movies[str(mid)]['Time']),
                "categories": ', '.join(movies[str(mid)]['Categories'])
            }

    helpers.bulk(es, actions())


# command line invocation builds index and prints the running time.
def main():
    start_time = time.time()
    buildIndex()
    print("=== Built index in %s seconds ===" % (time.time() - start_time))


if __name__ == '__main__':
    main()
