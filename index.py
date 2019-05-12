# Build index from the AZ-restaurant-reviews.json corpus for Elasticsearch.

import json, time
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch_dsl import Index, Document, Text, Keyword, Integer, Completion
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.analysis import analyzer, token_filter
from itertools import permutations

# Connect to local host server
connections.create_connection(hosts=['127.0.0.1'])

# Create elasticsearch object
es = Elasticsearch()

# Define analyzers: using a token_filter to handle Super-man, super man as superman
my_word_delimiter = token_filter('my_word_delimiter', type='word_delimiter', preserve_original=True, catenate_all=True)

my_analyzer = analyzer('custom1',
                       tokenizer='standard',
                       filter=['lowercase', 'stop', my_word_delimiter])

# text analyzer contains stemmer and lowercase thing
text_analyzer = analyzer('custom2',
                         tokenizer='letter',
                         filter=["stop", "lowercase", "porter_stem", "asciifolding", my_word_delimiter]
                         )

# the folding analyzer which only use lowercase and ascii-folding
folding_analyzer = analyzer('custom3',
                            tokenizer='standard',
                            filter=["lowercase", "asciifolding", my_word_delimiter])

# the category analyzer which use lowercase, ascii-folding and stemming
cat_analyzer = analyzer('custom4',
                        tokenizer='standard',
                        filter=["lowercase", "asciifolding", "porter_stem", my_word_delimiter])

ascii_fold = analyzer(
    'ascii_fold',
    # we don't want to split O'Brian or Toulouse-Lautrec
    tokenizer='whitespace',
    filter=[
        'lowercase',
        token_filter('ascii_fold', 'asciifolding')
    ]
)


# Define document mapping by defining the restaurant object
# and process fields and properties using analyzers defined above.
class Restaurant(Document):
    name = Text(fields={'keyword': Keyword()})
    suggest = Completion(analyzer=ascii_fold)  # for autocomplete
    review = Text(analyzer=text_analyzer)
    star = Integer()
    review_count = Integer()
    cool = Integer()
    useful = Integer()
    funny = Integer()
    city = Keyword()
    state = Keyword()
    address = Keyword()
    date = Keyword()
    postcode = Integer()

    def clean(self):
        """
        Automatically construct the suggestion input and weight by taking all
        possible permutation of Restaurant's name as ``input`` and taking their
        average review stars as ``weight``.
        """
        self.suggest = {
            'input': [' '.join(p) for p in permutations(self.name.split())],
            'weight': self.star
        }

    # override the Document save method to include subclass field definitions
    def save(self, *args, **kwargs):
        return super(Restaurant, self).save(*args, **kwargs)


# Populate the index
# when number is float or integer, return directly, else if it is a string of digit, parse, else just return 0 instead
def get_num(num):
    if isinstance(num, float) or isinstance(num, int):
        return num
    else:
        if num.isdigit():
            return float(num)
        else:
            return 0


def buildIndex():
    """
    buildIndex creates a new restaurant index, deleting any existing index of the same name.
    It loads a json file containing the movie corpus and does bulk loading
    using a generator function.
    """
    restaurant_index = Index('sample_restaurant_index')
    if restaurant_index.exists():
        restaurant_index.delete()  # Overwrite any previous version
    # create mapping from index to document
    restaurant_index.document(Restaurant)
    restaurant_index.create()

    # Open the json restaurant corpus
    with open('az_restaurant_reviews.json', 'r', encoding='utf-8') as data_file:
        # load restaurant from json file into dictionary
        restaurants = json.load(data_file)
        size = len(restaurants)

    # Action series for bulk loading with helpers.bulk function.
    # Implemented as a generator, to return one restaurant with each call.
    # Note that we include the index name here.
    # The Document type is always 'doc'.
    # Every item to be indexed must have a unique key.
    def actions():
        # mid is restaurant id (used as key into movies dictionary)
        for mid in range(1, size + 1):
            yield {
                "_index": "sample_restaurant_index",
                "_type": 'doc',
                "_id": mid - 1,
                "name": restaurants[str(mid - 1)]['business_name'],
                "suggest": restaurants[str(mid - 1)]['business_name'],
                "review": restaurants[str(mid - 1)]['review'],
                "address": restaurants[str(mid - 1)]['address'],
                "city": restaurants[str(mid - 1)]['city'],
                "star": get_num(restaurants[str(mid - 1)]['stars']),
                "state": restaurants[str(mid - 1)]['state'],
                "review_count": get_num(restaurants[str(mid - 1)]['review_count']),
                "useful": get_num(restaurants[str(mid - 1)]['useful']),
                "cool": get_num(restaurants[str(mid - 1)]['cool']),
                "funny": get_num(restaurants[str(mid - 1)]['funny']),
                "date": restaurants[str(mid - 1)]['date'],
                "postcode": restaurants[str(mid - 1)]['postal_code']
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
