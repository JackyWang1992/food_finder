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
my_word_delimiter = token_filter('my_word_delimiter', type='word_delimiter', preserve_original=True, catenate_all=True)

my_analyzer = analyzer('custom1',
                       tokenizer='standard',
                       filter=['lowercase', 'stop',my_word_delimiter])



# --- Add more analyzers here ---
# use stopwords... or not?
# use stemming... or not?
# the analyzer which tokenize for text, use stopwords stemming, lowercase and ascii-folding
text_analyzer = analyzer('custom2',
                         tokenizer='letter',
                         filter=["stop", "lowercase", "porter_stem", "asciifolding",my_word_delimiter]
                         )
# the folding analyzer which only use lowercase and ascii-folding
folding_analyzer = analyzer('custom3',
                            tokenizer='standard',
                            filter=["lowercase", "asciifolding",my_word_delimiter])
# the category analyzer which use lowercase, ascii-folding and stemming
cat_analyzer = analyzer('custom4',
                        tokenizer='standard',
                        filter=["lowercase", "asciifolding", "porter_stem",my_word_delimiter])


# Define document mapping (schema) by defining a class as a subclass of Document.
# This defines fields and their properties (type and analysis applied).
# You can use existing es analyzers or use ones you define yourself as above.
class Restaurant(Document):
    name = Text(analyzer=text_analyzer)
    # title_suggest = Completion()  # for autocomplete
    review = Text(analyzer=text_analyzer)
    # star = Text(analyzer=folding_analyzer)
    star = Integer()
    review_count = Integer()
    cool = Integer()
    useful = Integer()
    funny = Integer()
    city = Keyword()
    state = Keyword()
    address = Keyword()
    date = Keyword()

    # time = Text(analyzer='simple')
    # categories = Text(analyzer=cat_analyzer)

    # --- Add more fields here ---
    # What data type for your field? List?
    # Which analyzer makes sense for each field?

    # override the Document save method to include subclass field definitions
    def save(self, *args, **kwargs):
        return super(Restaurant, self).save(*args, **kwargs)


# Populate the index
# when time is not digit, just return 0 instead
def get_num(num):
    if isinstance(num, float):
        return float(num)
    else:
        return 0


def buildIndex():
    """
    buildIndex creates a new film index, deleting any existing index of
    the same name.
    It loads a json file containing the movie corpus and does bulk loading
    using a generator function.
    """
    restaurant_index = Index('sample_restaurant_index')
    if restaurant_index.exists():
        restaurant_index.delete()  # Overwrite any previous version
    # create mapping from index to document
    restaurant_index.document(Restaurant)
    restaurant_index.create()

    # Open the json film corpus
    with open('az_restaurant_reviews.json', 'r', encoding='utf-8') as data_file:
        # load movies from json file into dictionary
        restaurants = json.load(data_file)
        size = len(restaurants)
        # print(size)
        # print(restaurants['5']['review'])

    # Action series for bulk loading with helpers.bulk function.
    # Implemented as a generator, to return one movie with each call.
    # Note that we include the index name here.
    # The Document type is always 'doc'.
    # Every item to be indexed must have a unique key.
    def actions():
        # mid is movie id (used as key into movies dictionary)
        for mid in range(1, size + 1):
            yield {
                "_index": "sample_restaurant_index",
                "_type": 'doc',
                "_id": mid - 1,
                "name": restaurants[str(mid - 1)]['business_name'],
                # "name_suggest": restaurants[str(mid)]['business_name'],
                "review": restaurants[str(mid - 1)]['review'],
                "address": restaurants[str(mid - 1)]['address'],
                "city": restaurants[str(mid - 1)]['city'],  # You would like to convert runtime to
                # integer (in minutes) --- Add more fields here ---
                "star": get_num(restaurants[str(mid - 1)]['stars']),
                "state": restaurants[str(mid - 1)]['state'],
                "review_count": get_num(restaurants[str(mid - 1)]['review_count']),
                "useful": get_num(restaurants[str(mid - 1)]['useful']),
                "cool": get_num(restaurants[str(mid - 1)]['cool']),
                "funny": get_num(restaurants[str(mid - 1)]['funny']),
                "date": restaurants[str(mid - 1)]['date']
            }

    helpers.bulk(es, actions())
    print(restaurant_index)


# command line invocation builds index and prints the running time.
def main():
    start_time = time.time()
    buildIndex()
    print("=== Built index in %s seconds ===" % (time.time() - start_time))


if __name__ == '__main__':
    main()
